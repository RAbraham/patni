"""Patni - Pipe-like syntax for Python.

Lazy/parallel implementation using dataflow paradigm.

Usage:
    from patni.parallel import do, it, el, map, filter, reduce

    square = lambda x: x * x
    pipeline = 3 >> do(square)  # returns Deferred, not executed yet
    result = pipeline()          # triggers execution, result = 9

    # Parallel execution of independent branches:
    a = 3 >> do(square)          # Deferred
    b = 4 >> do(square)          # Deferred
    c = (a, b) >> do(lambda t: t[0] + t[1])
    result = c()                 # a and b execute in parallel

    # Using placeholder for entire value
    result = ({'name': 'alice'} >> do(it['name'].upper()))()  # 'ALICE'

    # Map, filter, reduce (lazy) - use `el` for individual elements
    result = ([1, 2, 3] >> map(el * 2))()  # [2, 4, 6]
    result = ([1, 2, 3, 4] >> filter(el > 2))()  # [3, 4]
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Tuple, Union, Optional
from functools import reduce as builtin_reduce
import sys

from .placeholder import Placeholder, it, el, resolve_placeholder


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

    def __rshift__(self, other):
        """Allow chaining: deferred >> pipeable"""
        if not isinstance(other, Pipeable):
            # Let other handle it via __rrshift__
            return NotImplemented
        if self._pipeline is None:
            return Deferred(self._value, other)
        else:
            # Compose pipelines
            composed = self._pipeline >> other
            return Deferred(self._value, composed)


class Pipeable:
    """Represents a composable, lazy pipeline step."""

    def __init__(self, func: Callable, *args, label: str = None, _composed: list = None):
        self.func = func
        self.args = args
        self.label = label
        # Track composed pipeables for visualization
        self._composed = _composed

    def _apply(self, value: Any) -> Any:
        """Apply this pipeline step to a value, resolving any Deferred/Placeholder args."""
        # If func is a Placeholder, apply it directly
        if isinstance(self.func, Placeholder):
            return self.func._apply(value)

        # Resolve args
        resolved_args = []
        deferred_indices = []

        for i, arg in enumerate(self.args):
            if isinstance(arg, Placeholder):
                resolved_args.append(arg._apply(value))
            elif isinstance(arg, Deferred):
                resolved_args.append(arg)  # placeholder for now
                deferred_indices.append(i)
            else:
                resolved_args.append(arg)

        # Resolve Deferred arguments in parallel if multiple
        if len(deferred_indices) > 1:
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.args[i]): i for i in deferred_indices}
                for future in as_completed(futures):
                    idx = futures[future]
                    resolved_args[idx] = future.result()
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

    def __rshift__(self, other):
        """Compose two Pipeables: self >> other"""
        if not isinstance(other, Pipeable):
            # Let other handle it via __rrshift__
            return NotImplemented
        def composed(value):
            intermediate = self._apply(value)
            return other._apply(intermediate)
        # Track the chain for visualization
        self_chain = self._composed if self._composed else [self]
        other_chain = other._composed if other._composed else [other]
        return Pipeable(composed, _composed=self_chain + other_chain)


def do(func_or_placeholder, *args, label: str = None) -> Pipeable:
    """Wrap a function or placeholder for use with pipe syntax (lazy evaluation).

    Args:
        func_or_placeholder: The function to wrap, or a Placeholder expression
        *args: Arguments to pass to the function. Use `it` as placeholder
               for the piped value. Deferred objects are resolved at
               execution time.
        label: Optional label for visualization.

    Returns:
        A Pipeable that can be used with >> operator

    Examples:
        # With function
        (3 >> do(lambda x: x * x))()  # 9

        # With placeholder
        ({'key': 'value'} >> do(it['key']))()  # 'value'
    """
    return Pipeable(func_or_placeholder, *args, label=label)


class MapPipeable:
    """Pipeable that maps a function over an iterable (lazy)."""

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
    """Pipeable that filters an iterable (lazy)."""

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
    """Pipeable that reduces an iterable (lazy)."""

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
    """Map a function over each element (lazy).

    Usage:
        ([1, 2, 3] >> map(lambda x: x * 2))()  # [2, 4, 6]
        ([1, 2, 3] >> map(el * 2))()           # [2, 4, 6]
    """
    return MapPipeable(func_or_placeholder)


def filter(func_or_placeholder):
    """Filter elements by a predicate (lazy).

    Usage:
        ([1, 2, 3, 4] >> filter(lambda x: x > 2))()  # [3, 4]
        ([1, 2, 3, 4] >> filter(el > 2))()           # [3, 4]
    """
    return FilterPipeable(func_or_placeholder)


def reduce(func, initial=None):
    """Reduce an iterable to a single value (lazy).

    Usage:
        ([1, 2, 3, 4] >> reduce(lambda a, b: a + b))()  # 10
    """
    return ReducePipeable(func, initial)


def _get_func_name(func: Callable) -> str:
    """Get a display name for a function."""
    if hasattr(func, '__name__') and func.__name__ != '<lambda>':
        return func.__name__
    return 'func'


def _collect_dag_nodes(obj, nodes: list, edges: list, node_id_map: dict, counter: list):
    """Recursively collect nodes and edges from a Deferred/Pipeable DAG."""
    if id(obj) in node_id_map:
        return node_id_map[id(obj)]

    node_id = f"n{counter[0]}"
    counter[0] += 1
    node_id_map[id(obj)] = node_id

    if isinstance(obj, Deferred):
        # Process the value
        value = obj._value
        if isinstance(value, tuple):
            # Tuple of values/Deferreds - parallel branches
            child_ids = []
            for item in value:
                if isinstance(item, Deferred):
                    child_id = _collect_dag_nodes(item, nodes, edges, node_id_map, counter)
                    child_ids.append(child_id)
                else:
                    val_id = f"n{counter[0]}"
                    counter[0] += 1
                    nodes.append((val_id, repr(item), 'value'))
                    child_ids.append(val_id)

            # Create merge node
            merge_id = f"n{counter[0]}"
            counter[0] += 1
            nodes.append((merge_id, 'merge (parallel)', 'merge'))
            for child_id in child_ids:
                edges.append((child_id, merge_id))

            # Process pipeline if present
            if obj._pipeline is not None:
                pipe_id = _collect_dag_nodes(obj._pipeline, nodes, edges, node_id_map, counter)
                edges.append((merge_id, pipe_id))
                node_id_map[id(obj)] = pipe_id
                return pipe_id
            else:
                node_id_map[id(obj)] = merge_id
                return merge_id
        elif isinstance(value, Deferred):
            child_id = _collect_dag_nodes(value, nodes, edges, node_id_map, counter)
            if obj._pipeline is not None:
                pipe_id = _collect_dag_nodes(obj._pipeline, nodes, edges, node_id_map, counter)
                edges.append((child_id, pipe_id))
                node_id_map[id(obj)] = pipe_id
                return pipe_id
            return child_id
        else:
            # Simple value
            val_id = f"n{counter[0]}"
            counter[0] += 1
            nodes.append((val_id, repr(value), 'value'))

            if obj._pipeline is not None:
                pipe_id = _collect_dag_nodes(obj._pipeline, nodes, edges, node_id_map, counter)
                edges.append((val_id, pipe_id))
                node_id_map[id(obj)] = pipe_id
                return pipe_id
            return val_id

    elif isinstance(obj, Pipeable):
        # Check if this is a composed pipeline
        if obj._composed:
            # Process each pipeable in the chain
            prev_id = None
            first_id = None
            for p in obj._composed:
                p_id = _collect_dag_nodes(p, nodes, edges, node_id_map, counter)
                if first_id is None:
                    first_id = p_id
                if prev_id is not None:
                    edges.append((prev_id, p_id))
                prev_id = p_id
            node_id_map[id(obj)] = prev_id  # Last node in chain
            return first_id if first_id else node_id

        # Get label or function name
        if obj.label:
            label = obj.label
        else:
            label = _get_func_name(obj.func)

        nodes.append((node_id, label, 'func'))

        # Check for Deferred args
        for arg in obj.args:
            if isinstance(arg, Deferred):
                arg_id = _collect_dag_nodes(arg, nodes, edges, node_id_map, counter)
                edges.append((arg_id, node_id))

        return node_id

    return node_id


def _format_simple(nodes: list, edges: list) -> str:
    """Format DAG as simple text."""
    if not nodes:
        return ""

    # Build adjacency for topological traversal
    from collections import defaultdict
    children = defaultdict(list)
    parents = defaultdict(list)
    for src, dst in edges:
        children[src].append(dst)
        parents[dst].append(src)

    # Find roots (nodes with no parents)
    all_nodes = {n[0] for n in nodes}
    roots = [n for n in all_nodes if n not in parents or not parents[n]]

    # Simple linear representation
    node_labels = {n[0]: n[1] for n in nodes}
    lines = []

    def traverse(node_id, visited):
        if node_id in visited:
            return []
        visited.add(node_id)
        result = [node_labels.get(node_id, node_id)]
        for child in children.get(node_id, []):
            child_results = traverse(child, visited)
            result.extend(child_results)
        return result

    visited = set()
    for root in roots:
        parts = traverse(root, visited)
        if parts:
            lines.append(" -> ".join(parts))

    return "\n".join(lines) if lines else " -> ".join(node_labels.values())


def _format_ascii(nodes: list, edges: list) -> str:
    """Format DAG as ASCII art."""
    if not nodes:
        return ""

    node_labels = {n[0]: n[1] for n in nodes}

    from collections import defaultdict
    children = defaultdict(list)
    parents = defaultdict(list)
    for src, dst in edges:
        children[src].append(dst)
        parents[dst].append(src)

    all_node_ids = {n[0] for n in nodes}
    roots = [n for n in all_node_ids if n not in parents]

    lines = []
    visited = set()

    def render(node_id, is_first=True):
        if node_id in visited:
            return
        visited.add(node_id)

        label = node_labels.get(node_id, node_id)
        if not is_first:
            lines.append("  │")
            lines.append("  ▼")
        lines.append(f"[{label}]")

        kids = children.get(node_id, [])
        for child in kids:
            render(child, is_first=False)

    for root in roots:
        render(root, is_first=True)

    return "\n".join(lines)


def _format_dot(nodes: list, edges: list) -> str:
    """Format DAG as Graphviz DOT."""
    lines = ["digraph pipeline {"]
    lines.append("  rankdir=TB;")

    for node_id, label, node_type in nodes:
        shape = "box" if node_type == 'func' else "ellipse"
        lines.append(f'  {node_id} [label="{label}" shape={shape}];')

    for src, dst in edges:
        lines.append(f"  {src} -> {dst};")

    lines.append("}")
    return "\n".join(lines)


class VisualizePipeable:
    """A pipeable that visualizes the DAG."""

    def __init__(self, format: str = "simple", output: str = None,
                 file: Union[str, Any] = None, show_parallel: bool = False):
        if format not in ("simple", "ascii", "dot"):
            raise ValueError(f"Invalid format: {format}. Must be 'simple', 'ascii', or 'dot'")
        self.format = format
        self.output = output
        self.file = file
        self.show_parallel = show_parallel

    def _visualize(self, obj) -> str:
        """Generate visualization string."""
        nodes = []
        edges = []
        node_id_map = {}
        counter = [0]

        _collect_dag_nodes(obj, nodes, edges, node_id_map, counter)

        if self.format == "simple":
            result = _format_simple(nodes, edges)
        elif self.format == "ascii":
            result = _format_ascii(nodes, edges)
        elif self.format == "dot":
            result = _format_dot(nodes, edges)
        else:
            result = _format_simple(nodes, edges)

        if self.show_parallel and "parallel" not in result.lower():
            # Add parallel indicator if there are merge nodes
            if any(n[2] == 'merge' for n in nodes):
                result = "# Parallel branches detected\n" + result

        return result

    def __rrshift__(self, obj):
        """Handle: obj >> visualize()"""
        viz_str = self._visualize(obj)

        if self.output == "return":
            return viz_str

        if self.file is not None:
            if isinstance(self.file, str):
                with open(self.file, 'w') as f:
                    f.write(viz_str)
            else:
                self.file.write(viz_str)
        else:
            print(viz_str)

        return obj


def visualize(format: str = "simple", output: str = None,
              file: Union[str, Any] = None, show_parallel: bool = False) -> VisualizePipeable:
    """Create a visualization pipeable.

    Args:
        format: Output format - 'simple', 'ascii', or 'dot'
        output: If 'return', returns the visualization string instead of the Deferred
        file: File path or file object to write to
        show_parallel: If True, explicitly mark parallel execution points

    Returns:
        A VisualizePipeable that can be used with >> operator

    Example:
        dag = 3 >> do(lambda x: x * x)
        dag >> visualize()  # prints to stdout
        dag >> visualize(format="dot", output="return")  # returns DOT string
    """
    return VisualizePipeable(format=format, output=output, file=file, show_parallel=show_parallel)


__all__ = ['do', 'it', 'el', 'map', 'filter', 'reduce', 'Deferred', 'visualize', 'Placeholder']
