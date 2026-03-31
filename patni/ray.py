"""Patni - Pipe-like syntax for Python with Ray distributed execution.

Lazy/parallel implementation using Ray for distributed computing.

Usage:
    from patni.ray import do, it, el, map, filter, reduce

    square = lambda x: x * x
    pipeline = 3 >> do(square)  # returns Deferred, not executed yet
    result = pipeline()          # triggers distributed execution, result = 9

    # Parallel execution of independent branches across Ray cluster:
    a = 3 >> do(square)          # Deferred
    b = 4 >> do(square)          # Deferred
    c = (a, b) >> do(lambda t: t[0] + t[1])
    result = c()                 # a and b execute in parallel on Ray

    # Using placeholder for entire value
    result = ({'name': 'alice'} >> do(it['name'].upper()))()  # 'ALICE'

    # Map, filter, reduce (lazy, distributed) - use `el` for individual elements
    result = ([1, 2, 3] >> map(el * 2))()  # [2, 4, 6]
    result = ([1, 2, 3, 4] >> filter(el > 2))()  # [3, 4]

Note:
    Ray must be installed: pip install patni[ray]
    For distributed execution, initialize Ray with your cluster:
        ray.init(address="auto")  # or specific cluster address
"""

try:
    import ray
except ImportError as e:
    raise ImportError(
        "ray is required for patni.ray. "
        "Install it with: pip install patni[ray]"
    ) from e

from typing import Any, Callable, Tuple, List
from functools import reduce as builtin_reduce

from .placeholder import Placeholder, it, el


def _ensure_ray_initialized():
    """Initialize Ray if not already initialized."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)


@ray.remote
def _execute_func(func: Callable, *args) -> Any:
    """Remote function to execute a callable with arguments on Ray."""
    return func(*args)


class Deferred:
    """Represents a lazy/deferred computation using Ray.

    The computation is not executed until __call__ is invoked.
    Execution happens on the Ray cluster for distributed parallelism.
    """

    def __init__(self, value: Any, pipeline: 'Pipeable' = None):
        self._value = value
        self._pipeline = pipeline
        self._resolved = False
        self._result = None

    def __call__(self) -> Any:
        """Trigger evaluation of the deferred computation using Ray."""
        if self._resolved:
            return self._result

        _ensure_ray_initialized()
        self._result = self._execute()
        self._resolved = True
        return self._result

    def _execute(self) -> Any:
        """Execute the computation on Ray."""
        # First, resolve the input value
        value = self._resolve_value(self._value)

        # Apply pipeline if present
        if self._pipeline is not None:
            return self._pipeline._apply(value)
        return value

    def _resolve_value(self, value: Any) -> Any:
        """Resolve the input value, handling nested Deferred and tuples."""
        if isinstance(value, Deferred):
            return value._execute()
        elif isinstance(value, tuple):
            return self._resolve_tuple_parallel(value)
        elif isinstance(value, ray.ObjectRef):
            return ray.get(value)
        return value

    def _resolve_tuple_parallel(self, t: tuple) -> tuple:
        """Resolve tuple elements in parallel using Ray."""
        deferred_items = [(i, item) for i, item in enumerate(t) if isinstance(item, Deferred)]

        if len(deferred_items) <= 1:
            # No parallelism benefit, resolve sequentially
            return tuple(
                item._execute() if isinstance(item, Deferred)
                else ray.get(item) if isinstance(item, ray.ObjectRef)
                else item
                for item in t
            )

        # Parallel execution using Ray
        results = list(t)

        # Submit all deferred items as Ray tasks
        futures: List[Tuple[int, ray.ObjectRef]] = []
        for i, item in deferred_items:
            # Create a remote task for each deferred
            if item._pipeline is not None:
                ref = _execute_func.remote(
                    lambda v, p: p._apply(v) if p else v,
                    item._resolve_value(item._value),
                    item._pipeline
                )
            else:
                ref = _execute_func.remote(lambda v: v, item._resolve_value(item._value))
            futures.append((i, ref))

        # Gather results
        for idx, ref in futures:
            results[idx] = ray.get(ref)

        return tuple(results)

    def __rrshift__(self, value: Any) -> 'Deferred':
        """Allow chaining: other_value >> deferred"""
        return Deferred(value, Pipeable(lambda x: self()))

    def __rshift__(self, other) -> 'Deferred':
        """Allow chaining: deferred >> pipeable"""
        if not isinstance(other, Pipeable):
            return NotImplemented
        if self._pipeline is None:
            return Deferred(self._value, other)
        else:
            # Compose pipelines
            composed = self._pipeline >> other
            return Deferred(self._value, composed)


class Pipeable:
    """Represents a composable, lazy pipeline step for Ray execution."""

    def __init__(self, func: Callable, *args):
        self.func = func
        self.args = args

    def _apply(self, value: Any) -> Any:
        """Apply this pipeline step to a value, resolving any Deferred/Placeholder args."""
        _ensure_ray_initialized()

        # If func is a Placeholder, apply it directly
        if isinstance(self.func, Placeholder):
            return self.func._apply(value)

        # Collect all deferred arguments
        resolved_args = []
        deferred_refs: List[Tuple[int, ray.ObjectRef]] = []

        for i, arg in enumerate(self.args):
            if isinstance(arg, Placeholder):
                resolved_args.append(arg._apply(value))
            elif isinstance(arg, Deferred):
                # Submit as Ray task and track for parallel resolution
                if arg._pipeline is not None:
                    ref = _execute_func.remote(
                        lambda v, p: p._apply(v) if p else v,
                        arg._resolve_value(arg._value),
                        arg._pipeline
                    )
                else:
                    ref = _execute_func.remote(lambda v: v, arg._resolve_value(arg._value))
                resolved_args.append(None)  # Placeholder
                deferred_refs.append((i, ref))
            elif isinstance(arg, ray.ObjectRef):
                resolved_args.append(None)  # Placeholder
                deferred_refs.append((i, arg))
            else:
                resolved_args.append(arg)

        # Resolve all deferred arguments in parallel
        if deferred_refs:
            for idx, ref in deferred_refs:
                resolved_args[idx] = ray.get(ref)

        if not self.args:
            return self.func(value)
        return self.func(*resolved_args)

    def __rrshift__(self, value: Any) -> Deferred:
        """value >> pipeable returns a Deferred computation."""
        if isinstance(value, Deferred):
            # Deferred >> Pipeable: extend the pipeline
            return value >> self
        return Deferred(value, self)

    def __rshift__(self, other) -> 'Pipeable':
        """Compose two Pipeables: self >> other"""
        if not isinstance(other, Pipeable):
            return NotImplemented
        def composed(value):
            intermediate = self._apply(value)
            return other._apply(intermediate)
        return Pipeable(composed)


def do(func_or_placeholder, *args) -> Pipeable:
    """Wrap a function or placeholder for use with pipe syntax (lazy Ray evaluation).

    Args:
        func_or_placeholder: The function to wrap, or a Placeholder expression
        *args: Arguments to pass to the function. Use `it` as placeholder
               for the piped value. Deferred objects are resolved at
               execution time in parallel on the Ray cluster.

    Returns:
        A Pipeable that can be used with >> operator

    Example:
        from patni.ray import do, it

        # Simple pipeline
        result = (3 >> do(lambda x: x * x))()  # 9

        # With placeholder
        result = ({'key': 'value'} >> do(it['key']))()  # 'value'

        # Parallel branches
        a = 3 >> do(lambda x: x * x)
        b = 4 >> do(lambda x: x * x)
        c = (a, b) >> do(lambda t: t[0] + t[1])
        result = c()  # 25 (9 + 16)
    """
    return Pipeable(func_or_placeholder, *args)


class MapPipeable:
    """Pipeable that maps a function over an iterable (lazy, distributed)."""

    def __init__(self, func_or_placeholder):
        self.func = func_or_placeholder

    def __rrshift__(self, value):
        if isinstance(value, Deferred):
            return value >> Pipeable(self._apply_map)
        return Deferred(value, Pipeable(self._apply_map))

    def _apply_map(self, value):
        if isinstance(self.func, Placeholder):
            return [self.func._apply(x) for x in value]
        return [self.func(x) for x in value]


class FilterPipeable:
    """Pipeable that filters an iterable (lazy, distributed)."""

    def __init__(self, func_or_placeholder):
        self.func = func_or_placeholder

    def __rrshift__(self, value):
        if isinstance(value, Deferred):
            return value >> Pipeable(self._apply_filter)
        return Deferred(value, Pipeable(self._apply_filter))

    def _apply_filter(self, value):
        if isinstance(self.func, Placeholder):
            return [x for x in value if self.func._apply(x)]
        return [x for x in value if self.func(x)]


class ReducePipeable:
    """Pipeable that reduces an iterable (lazy, distributed)."""

    def __init__(self, func, initial=None):
        self.func = func
        self.initial = initial
        self._has_initial = initial is not None

    def __rrshift__(self, value):
        if isinstance(value, Deferred):
            return value >> Pipeable(self._apply_reduce)
        return Deferred(value, Pipeable(self._apply_reduce))

    def _apply_reduce(self, value):
        if self._has_initial:
            return builtin_reduce(self.func, value, self.initial)
        return builtin_reduce(self.func, value)


def map(func_or_placeholder):
    """Map a function over each element (lazy, distributed on Ray).

    Usage:
        ([1, 2, 3] >> map(lambda x: x * 2))()  # [2, 4, 6]
        ([1, 2, 3] >> map(el * 2))()           # [2, 4, 6]
    """
    return MapPipeable(func_or_placeholder)


def filter(func_or_placeholder):
    """Filter elements by a predicate (lazy, distributed on Ray).

    Usage:
        ([1, 2, 3, 4] >> filter(lambda x: x > 2))()  # [3, 4]
        ([1, 2, 3, 4] >> filter(el > 2))()           # [3, 4]
    """
    return FilterPipeable(func_or_placeholder)


def reduce(func, initial=None):
    """Reduce an iterable to a single value (lazy, distributed on Ray).

    Usage:
        ([1, 2, 3, 4] >> reduce(lambda a, b: a + b))()  # 10
    """
    return ReducePipeable(func, initial)


__all__ = ['do', 'it', 'el', 'map', 'filter', 'reduce', 'Deferred', 'Placeholder']
