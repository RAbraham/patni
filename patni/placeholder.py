"""Placeholder implementation for pipe-like syntax.

The `it` object acts as a placeholder for the entire piped value, supporting:
- Item access: it['key'], it[0]
- Attribute access: it.name
- Method calls: it.upper()
- Operators: it + 1, it > 5, it * 2

The `el` object acts as a placeholder for individual elements in map/filter:
- [1, 2, 3] >> map(el * 2)     # [2, 4, 6]
- [1, 2, 3, 4] >> filter(el > 2)  # [3, 4]
"""

from functools import reduce as builtin_reduce


class Placeholder:
    """A placeholder that records operations to be applied to a value later."""

    __slots__ = ('_ops',)

    def __init__(self, ops=None):
        object.__setattr__(self, '_ops', ops if ops is not None else [])

    def _apply(self, value):
        """Apply all recorded operations to the value."""
        result = value
        for op in self._ops:
            result = op(result)
        return result

    def _extend(self, op):
        """Create a new Placeholder with an additional operation."""
        return Placeholder(self._ops + [op])

    def __getitem__(self, key):
        return self._extend(lambda x, k=key: x[k])

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return _AttrPlaceholder(self._ops, name)

    # Arithmetic operators
    def __add__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) + o._apply(x)])
        return self._extend(lambda x, o=other: x + o)

    def __radd__(self, other):
        return self._extend(lambda x, o=other: o + x)

    def __sub__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) - o._apply(x)])
        return self._extend(lambda x, o=other: x - o)

    def __rsub__(self, other):
        return self._extend(lambda x, o=other: o - x)

    def __mul__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) * o._apply(x)])
        return self._extend(lambda x, o=other: x * o)

    def __rmul__(self, other):
        return self._extend(lambda x, o=other: o * x)

    def __truediv__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) / o._apply(x)])
        return self._extend(lambda x, o=other: x / o)

    def __rtruediv__(self, other):
        return self._extend(lambda x, o=other: o / x)

    def __floordiv__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) // o._apply(x)])
        return self._extend(lambda x, o=other: x // o)

    def __rfloordiv__(self, other):
        return self._extend(lambda x, o=other: o // x)

    def __mod__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) % o._apply(x)])
        return self._extend(lambda x, o=other: x % o)

    def __rmod__(self, other):
        return self._extend(lambda x, o=other: o % x)

    def __pow__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) ** o._apply(x)])
        return self._extend(lambda x, o=other: x ** o)

    def __rpow__(self, other):
        return self._extend(lambda x, o=other: o ** x)

    # Comparison operators
    def __lt__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) < o._apply(x)])
        return self._extend(lambda x, o=other: x < o)

    def __le__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) <= o._apply(x)])
        return self._extend(lambda x, o=other: x <= o)

    def __gt__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) > o._apply(x)])
        return self._extend(lambda x, o=other: x > o)

    def __ge__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) >= o._apply(x)])
        return self._extend(lambda x, o=other: x >= o)

    def __eq__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) == o._apply(x)])
        return self._extend(lambda x, o=other: x == o)

    def __ne__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) != o._apply(x)])
        return self._extend(lambda x, o=other: x != o)

    # Unary operators
    def __neg__(self):
        return self._extend(lambda x: -x)

    def __pos__(self):
        return self._extend(lambda x: +x)

    def __abs__(self):
        return self._extend(abs)

    def __invert__(self):
        return self._extend(lambda x: ~x)

    # Bitwise operators
    def __and__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) & o._apply(x)])
        return self._extend(lambda x, o=other: x & o)

    def __rand__(self, other):
        return self._extend(lambda x, o=other: o & x)

    def __or__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) | o._apply(x)])
        return self._extend(lambda x, o=other: x | o)

    def __ror__(self, other):
        return self._extend(lambda x, o=other: o | x)

    def __xor__(self, other):
        if isinstance(other, Placeholder):
            return Placeholder([lambda x, s=self, o=other: s._apply(x) ^ o._apply(x)])
        return self._extend(lambda x, o=other: x ^ o)

    def __rxor__(self, other):
        return self._extend(lambda x, o=other: o ^ x)

    def __lshift__(self, other):
        return self._extend(lambda x, o=other: x << o)

    def __rlshift__(self, other):
        return self._extend(lambda x, o=other: o << x)

    def __rrshift__(self, value):
        """Allow: value >> it[...] or value >> (it + 2)"""
        return self._apply(value)


class _AttrPlaceholder(Placeholder):
    """A placeholder representing attribute access that might be called as a method."""

    __slots__ = ('_attr_name', '_base_ops')

    def __init__(self, base_ops, attr_name):
        # Add getattr to ops
        super().__init__(base_ops + [lambda x, n=attr_name: getattr(x, n)])
        object.__setattr__(self, '_attr_name', attr_name)
        object.__setattr__(self, '_base_ops', base_ops)

    def __call__(self, *args, **kwargs):
        """Handle method calls like it.upper()"""
        def method_call(x, name=self._attr_name, a=args, kw=kwargs):
            return getattr(x, name)(*a, **kw)
        return Placeholder(self._base_ops + [method_call])


# Singleton instances
it = Placeholder()  # For the entire piped value
el = Placeholder()  # For individual elements in map/filter


def resolve_placeholder(arg, value):
    """Resolve a placeholder or return the arg as-is."""
    if isinstance(arg, Placeholder):
        return arg._apply(value)
    return arg


class MapPipeable:
    """Pipeable that maps a function over an iterable."""

    def __init__(self, func_or_placeholder):
        self.func = func_or_placeholder

    def __rrshift__(self, value):
        if isinstance(self.func, Placeholder):
            return [self.func._apply(x) for x in value]
        return [self.func(x) for x in value]


class FilterPipeable:
    """Pipeable that filters an iterable."""

    def __init__(self, func_or_placeholder):
        self.func = func_or_placeholder

    def __rrshift__(self, value):
        if isinstance(self.func, Placeholder):
            return [x for x in value if self.func._apply(x)]
        return [x for x in value if self.func(x)]


class ReducePipeable:
    """Pipeable that reduces an iterable."""

    def __init__(self, func_or_placeholder, initial=None):
        self.func = func_or_placeholder
        self.initial = initial
        self._has_initial = initial is not None

    def __rrshift__(self, value):
        if isinstance(self.func, Placeholder):
            raise TypeError("reduce does not support Placeholder; use a function")
        if self._has_initial:
            return builtin_reduce(self.func, value, self.initial)
        return builtin_reduce(self.func, value)


def map(func_or_placeholder):
    """Map a function over each element.

    Usage:
        [1, 2, 3] >> map(lambda x: x * 2)  # [2, 4, 6]
        [1, 2, 3] >> map(el * 2)           # [2, 4, 6]
    """
    return MapPipeable(func_or_placeholder)


def filter(func_or_placeholder):
    """Filter elements by a predicate.

    Usage:
        [1, 2, 3, 4] >> filter(lambda x: x > 2)  # [3, 4]
        [1, 2, 3, 4] >> filter(el > 2)           # [3, 4]
    """
    return FilterPipeable(func_or_placeholder)


def reduce(func, initial=None):
    """Reduce an iterable to a single value.

    Usage:
        [1, 2, 3, 4] >> reduce(lambda a, b: a + b)      # 10
        [1, 2, 3, 4] >> reduce(lambda a, b: a + b, 0)   # 10
    """
    return ReducePipeable(func, initial)
