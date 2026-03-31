"""Patni - Pipe-like syntax for Python.

Eager/sequential implementation.

Usage:
    from patni import do, it, el, map, filter, reduce

    # Basic piping
    result = 3 >> do(lambda x: x * x)  # 9

    # Using placeholder for entire value
    result = {'name': 'alice'} >> do(it['name'].upper())  # 'ALICE'

    # Map, filter, reduce (use `el` for individual elements)
    result = [1, 2, 3] >> map(el * 2)  # [2, 4, 6]
    result = [1, 2, 3, 4] >> filter(el > 2)  # [3, 4]
    result = [1, 2, 3, 4] >> reduce(lambda a, b: a + b)  # 10
"""

from .placeholder import (
    Placeholder,
    it,
    el,
    map,
    filter,
    reduce,
    resolve_placeholder,
)


class Pipeable:
    def __init__(self, func_or_placeholder, *args):
        self.func = func_or_placeholder
        self.args = args

    def __rrshift__(self, value):
        # If func is a Placeholder, apply it directly
        if isinstance(self.func, Placeholder):
            return self.func._apply(value)

        # Replace placeholders with resolved values
        resolved_args = [resolve_placeholder(arg, value) for arg in self.args]
        if not resolved_args:
            return self.func(value)
        return self.func(*resolved_args)

    def __rshift__(self, other):
        # Compose two Pipeables: self >> other
        if not isinstance(other, Pipeable):
            return NotImplemented
        def composed(value):
            intermediate = self.__rrshift__(value)
            return other.__rrshift__(intermediate)
        return Pipeable(composed)


def do(func_or_placeholder, *args):
    """Wrap a function or placeholder for use with pipe syntax.

    Args:
        func_or_placeholder: The function to wrap, or a Placeholder expression
        *args: Arguments to pass to the function. Use `it` as placeholder
               for the piped value.

    Returns:
        A Pipeable that can be used with >> operator

    Examples:
        # With function
        3 >> do(lambda x: x * x)  # 9

        # With placeholder
        {'key': 'value'} >> do(it['key'])  # 'value'

        # With placeholder in args
        9 >> do(lambda x, y: x - y, it, 2)  # 7
    """
    return Pipeable(func_or_placeholder, *args)


__all__ = ['do', 'it', 'el', 'map', 'filter', 'reduce', 'Placeholder']
