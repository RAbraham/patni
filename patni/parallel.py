"""Patni - Pipe-like syntax for Python.

Lazy/parallel implementation using dataflow paradigm.

Usage:
    from patni.parallel import do, it

    square = lambda x: x * x
    pipeline = 3 >> do(square)  # returns Deferred, not executed yet
    result = pipeline()          # triggers execution, result = 9

    # Parallel execution of independent branches:
    a = 3 >> do(square)          # Deferred
    b = 4 >> do(square)          # Deferred
    c = (a, b) >> do(lambda t: t[0] + t[1])
    result = c()                 # a and b execute in parallel
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Tuple

class _It:
    """Placeholder for the piped value."""
    pass

it = _It()


class Deferred:
    """Represents a lazy/deferred computation.

    The computation is not executed until __call__ is invoked.
    """

    def __init__(self, value: Any, pipeline: 'Pipeable' = None):
        self._value = value
        self._pipeline = pipeline
        self._resolved = False
        self._result = None

    def __call__(self) -> Any:
        """Trigger evaluation of the deferred computation."""
        if self._resolved:
            return self._result

        # First, resolve the input value if it's a Deferred
        value = self._value
        if isinstance(value, Deferred):
            value = value()
        elif isinstance(value, tuple):
            # Check for parallel opportunities in tuples
            value = self._resolve_tuple_parallel(value)

        # Apply pipeline if present
        if self._pipeline is not None:
            self._result = self._pipeline._apply(value)
        else:
            self._result = value

        self._resolved = True
        return self._result

    def _resolve_tuple_parallel(self, t: tuple) -> tuple:
        """Resolve tuple elements, running Deferred items in parallel."""
        deferred_items = [(i, item) for i, item in enumerate(t) if isinstance(item, Deferred)]

        if len(deferred_items) <= 1:
            # No parallelism benefit, resolve sequentially
            return tuple(item() if isinstance(item, Deferred) else item for item in t)

        # Parallel execution
        results = list(t)
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(item): i for i, item in deferred_items}
            for future in as_completed(futures):
                idx = futures[future]
                results[idx] = future.result()

        return tuple(results)

    def __rrshift__(self, value: Any) -> 'Deferred':
        """Allow chaining: other_value >> deferred"""
        # This shouldn't typically happen, but handle it
        return Deferred(value, Pipeable(lambda x: self()))

    def __rshift__(self, other: 'Pipeable') -> 'Deferred':
        """Allow chaining: deferred >> pipeable"""
        if self._pipeline is None:
            return Deferred(self._value, other)
        else:
            # Compose pipelines
            composed = self._pipeline >> other
            return Deferred(self._value, composed)


class Pipeable:
    """Represents a composable, lazy pipeline step."""

    def __init__(self, func: Callable, *args):
        self.func = func
        self.args = args

    def _apply(self, value: Any) -> Any:
        """Apply this pipeline step to a value, resolving any Deferred args."""
        # Resolve any Deferred objects in args
        resolved_args = []
        deferred_indices = []

        for i, arg in enumerate(self.args):
            if arg is it:
                resolved_args.append(value)
            elif isinstance(arg, Deferred):
                resolved_args.append(arg)  # placeholder
                deferred_indices.append(i)
            else:
                resolved_args.append(arg)

        # Resolve Deferred arguments in parallel if multiple
        if len(deferred_indices) > 1:
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.args[i]): i for i in deferred_indices}
                for future in as_completed(futures):
                    idx = futures[future]
                    # Find position in resolved_args
                    pos = list(self.args).index(self.args[idx])
                    resolved_args[pos] = future.result()
        elif len(deferred_indices) == 1:
            idx = deferred_indices[0]
            resolved_args[idx] = self.args[idx]()

        if not self.args:
            return self.func(value)
        return self.func(*resolved_args)

    def __rrshift__(self, value: Any) -> Deferred:
        """value >> pipeable returns a Deferred computation."""
        if isinstance(value, Deferred):
            # Deferred >> Pipeable: extend the pipeline
            return value >> self
        return Deferred(value, self)

    def __rshift__(self, other: 'Pipeable') -> 'Pipeable':
        """Compose two Pipeables: self >> other"""
        def composed(value):
            intermediate = self._apply(value)
            return other._apply(intermediate)
        return Pipeable(composed)


def do(func: Callable, *args) -> Pipeable:
    """Wrap a function for use with pipe syntax (lazy evaluation).

    Args:
        func: The function to wrap
        *args: Arguments to pass to the function. Use `it` as placeholder
               for the piped value. Deferred objects are resolved at
               execution time.

    Returns:
        A Pipeable that can be used with >> operator
    """
    return Pipeable(func, *args)


__all__ = ['do', 'it', 'Deferred']
