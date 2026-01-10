"""Patni - Ibis integration with pipe syntax.

Provides pipe-like syntax for Ibis expressions with lazy execution.

Usage:
    from patni.ibis import select, filter, group_by, agg, order_by, limit, it

    t = con.table("users")
    result = (
        t
        >> filter(it.age > 21)
        >> select(it.name, it.email)
        >> limit(10)
    )()  # Execute
"""

try:
    import ibis
    from ibis import _ as it
except ImportError as e:
    raise ImportError(
        "ibis is required for patni.ibis. "
        "Install it with: pip install patni[ibis]"
    ) from e

from typing import Any, Callable


class IbisDeferred:
    """Represents a lazy Ibis expression.

    The expression is not executed until __call__ is invoked.
    """

    def __init__(self, expr):
        self._expr = expr
        self._resolved = False
        self._result = None

    def __call__(self):
        """Execute the Ibis expression."""
        if self._resolved:
            return self._result
        self._result = self._expr.execute()
        self._resolved = True
        return self._result

    def __rshift__(self, other: 'IbisPipeable') -> 'IbisDeferred':
        """Chain: deferred >> pipeable"""
        new_expr = other._apply(self._expr)
        return IbisDeferred(new_expr)

    @property
    def expr(self):
        """Access the underlying Ibis expression (for inspection/debugging)."""
        return self._expr


class IbisPipeable:
    """Represents a composable Ibis operation."""

    def __init__(self, func: Callable):
        self.func = func

    def _apply(self, expr) -> Any:
        """Apply this operation to an Ibis expression."""
        return self.func(expr)

    def __rrshift__(self, expr) -> IbisDeferred:
        """expr >> pipeable returns an IbisDeferred."""
        if isinstance(expr, IbisDeferred):
            return expr >> self
        new_expr = self._apply(expr)
        return IbisDeferred(new_expr)

    def __rshift__(self, other: 'IbisPipeable') -> 'IbisPipeable':
        """Compose two IbisPipeables: self >> other"""
        def composed(expr):
            intermediate = self._apply(expr)
            return other._apply(intermediate)
        return IbisPipeable(composed)


# --- Ibis Operation Wrappers ---

def select(*cols):
    """Select columns from a table.

    Usage:
        t >> select(it.name, it.email)
        t >> select("name", "email")
    """
    return IbisPipeable(lambda t: t.select(*cols))


def filter(predicate):
    """Filter rows based on a predicate.

    Usage:
        t >> filter(it.age > 21)
        t >> filter((it.age > 21) & (it.status == "active"))
    """
    return IbisPipeable(lambda t: t.filter(predicate))


def group_by(*cols):
    """Group by columns.

    Usage:
        t >> group_by(it.category) >> agg(total=it.amount.sum())
        t >> group_by("category", "region")
    """
    return IbisPipeable(lambda t: t.group_by(*cols))


def agg(**kwargs):
    """Aggregate grouped data.

    Usage:
        t >> group_by(it.category) >> agg(
            total=it.amount.sum(),
            count=it.id.count()
        )
    """
    return IbisPipeable(lambda grouped: grouped.agg(**kwargs))


# Alias for agg
aggregate = agg


def order_by(*cols):
    """Order by columns.

    Usage:
        t >> order_by(it.name)
        t >> order_by(it.amount.desc())
    """
    return IbisPipeable(lambda t: t.order_by(*cols))


def limit(n: int):
    """Limit number of rows.

    Usage:
        t >> limit(10)
    """
    return IbisPipeable(lambda t: t.limit(n))


def mutate(**kwargs):
    """Add or modify columns.

    Usage:
        t >> mutate(full_name=it.first_name + " " + it.last_name)
    """
    return IbisPipeable(lambda t: t.mutate(**kwargs))


def distinct(*cols):
    """Get distinct rows.

    Usage:
        t >> distinct()
        t >> select(it.category) >> distinct()
    """
    if cols:
        return IbisPipeable(lambda t: t.distinct(*cols))
    return IbisPipeable(lambda t: t.distinct())


def join(right, predicate, how="inner"):
    """Join with another table.

    Usage:
        t1 >> join(t2, it.id == t2.user_id)
        t1 >> join(t2, it.id == t2.user_id, how="left")
    """
    return IbisPipeable(lambda left: left.join(right, predicate, how=how))


def head(n: int = 5):
    """Get first n rows (alias for limit).

    Usage:
        t >> head()
        t >> head(10)
    """
    return limit(n)


def drop(*cols):
    """Drop columns.

    Usage:
        t >> drop(it.password, it.secret)
        t >> drop("password", "secret")
    """
    return IbisPipeable(lambda t: t.drop(*cols))


def rename(**kwargs):
    """Rename columns.

    Usage:
        t >> rename(user_name=it.name)
    """
    return IbisPipeable(lambda t: t.rename(**kwargs))


def cast(col, dtype):
    """Cast a column to a different type.

    Usage:
        t >> mutate(age=cast(it.age, "int64"))
    """
    return col.cast(dtype)


__all__ = [
    'it',
    'select',
    'filter',
    'group_by',
    'agg',
    'aggregate',
    'order_by',
    'limit',
    'mutate',
    'distinct',
    'join',
    'head',
    'drop',
    'rename',
    'cast',
    'IbisDeferred',
    'IbisPipeable',
]
