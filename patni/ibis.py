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


# --- Window Functions ---

class WindowSpec:
    """Represents a window specification for window functions.

    Usage:
        w = window(partition_by=it.category, order_by=it.date)

        result = (
            t
            >> mutate(
                row_num=row_number(w),
                running_total=it.amount.sum() >> w,
            )
        )()
    """

    def __init__(
        self,
        partition_by=None,
        order_by=None,
        rows_between=None,
    ):
        self.partition_by = partition_by
        self.order_by = order_by
        self.rows_between = rows_between

    def _to_ibis(self):
        """Convert to ibis window object."""
        kwargs = {}

        if self.partition_by is not None:
            kwargs['group_by'] = self.partition_by

        if self.order_by is not None:
            kwargs['order_by'] = self.order_by

        if self.rows_between is not None:
            preceding, following = self.rows_between
            # Convert to ibis format: preceding is positive int or None (unbounded)
            if preceding is not None:
                kwargs['preceding'] = abs(preceding)
            else:
                kwargs['preceding'] = None  # unbounded
            if following is not None:
                kwargs['following'] = following
            else:
                kwargs['following'] = None  # unbounded

        return ibis.window(**kwargs)

    def __rrshift__(self, expr):
        """Apply window to expression: expr >> window_spec"""
        return expr.over(self._to_ibis())

    def __call__(self, expr):
        """Apply window to expression: w(expr)

        This is the recommended syntax for aggregations since >> conflicts
        with Ibis's bitwise operators on numeric expressions.

        Usage:
            w = window(partition_by=it.category, order_by=it.date)
            t >> mutate(running_total=w(it.amount.sum()))
        """
        return expr.over(self._to_ibis())


def window(partition_by=None, order_by=None, rows_between=None):
    """Create a window specification.

    Usage:
        # Partition by category, order by date
        w = window(partition_by=it.category, order_by=it.date)

        # Rolling window: current row and 2 preceding
        w = window(order_by=it.date, rows_between=(-2, 0))

        # Unbounded preceding to current (cumulative)
        w = window(order_by=it.date, rows_between=(None, 0))
    """
    return WindowSpec(partition_by, order_by, rows_between)


def row_number(window_spec):
    """Row number within the window partition (1-indexed).

    Usage:
        w = window(partition_by=it.category, order_by=it.date)
        t >> mutate(row_num=row_number(w))
    """
    return ibis.row_number().over(window_spec._to_ibis())


def rank(window_spec):
    """Rank with gaps for ties.

    Usage:
        w = window(order_by=it.score.desc())
        t >> mutate(ranking=rank(w))
    """
    return ibis.rank().over(window_spec._to_ibis())


def dense_rank(window_spec):
    """Rank without gaps for ties.

    Usage:
        w = window(order_by=it.score.desc())
        t >> mutate(ranking=dense_rank(w))
    """
    return ibis.dense_rank().over(window_spec._to_ibis())


def lag(column, offset, window_spec, default=None):
    """Access value from a previous row.

    Usage:
        w = window(partition_by=it.category, order_by=it.date)
        t >> mutate(prev_amount=lag(it.amount, 1, w))
        t >> mutate(prev_amount=lag(it.amount, 1, w, default=0))
    """
    return column.lag(offset, default=default).over(window_spec._to_ibis())


def lead(column, offset, window_spec, default=None):
    """Access value from a following row.

    Usage:
        w = window(partition_by=it.category, order_by=it.date)
        t >> mutate(next_amount=lead(it.amount, 1, w))
    """
    return column.lead(offset, default=default).over(window_spec._to_ibis())


def ntile(n, window_spec):
    """Divide rows into n roughly equal buckets.

    Usage:
        w = window(order_by=it.amount)
        t >> mutate(quartile=ntile(4, w))
    """
    return ibis.ntile(n).over(window_spec._to_ibis())


def first_value(column, window_spec):
    """Get the first value in the window.

    Usage:
        w = window(partition_by=it.category, order_by=it.date)
        t >> mutate(first_amount=first_value(it.amount, w))
    """
    return column.first().over(window_spec._to_ibis())


def last_value(column, window_spec):
    """Get the last value in the window.

    Usage:
        w = window(partition_by=it.category, order_by=it.date)
        t >> mutate(last_amount=last_value(it.amount, w))
    """
    return column.last().over(window_spec._to_ibis())


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
    # Window functions
    'window',
    'WindowSpec',
    'row_number',
    'rank',
    'dense_rank',
    'lag',
    'lead',
    'ntile',
    'first_value',
    'last_value',
    # Classes
    'IbisDeferred',
    'IbisPipeable',
]
