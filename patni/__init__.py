"""Patni - Pipe-like syntax for Python.

Eager/sequential implementation.

Usage:
    from patni import do, it

    square = lambda x: x * x
    result = 3 >> do(square)  # executes immediately, result = 9
"""

class _It:
    """Placeholder for the piped value."""
    pass

it = _It()

class Pipeable:
    def __init__(self, func, *args):
        self.func = func
        self.args = args

    def __rrshift__(self, value):
        # Replace `it` placeholders with the piped value
        resolved_args = [value if arg is it else arg for arg in self.args]
        if not resolved_args:
            return self.func(value)
        return self.func(*resolved_args)

    def __rshift__(self, other):
        # Compose two Pipeables: self >> other
        def composed(value):
            intermediate = self.__rrshift__(value)
            return other.__rrshift__(intermediate)
        return Pipeable(composed)

def do(func, *args):
    """Wrap a function for use with pipe syntax.

    Args:
        func: The function to wrap
        *args: Arguments to pass to the function. Use `it` as placeholder
               for the piped value.

    Returns:
        A Pipeable that can be used with >> operator
    """
    return Pipeable(func, *args)

__all__ = ['do', 'it']
