# Window Functions Plan for patni.ibis

## Overview

Window functions perform calculations across a set of rows related to the current row, without collapsing the result into a single output row (unlike `group_by` + `agg`).

Common use cases:
- Running totals
- Row numbering
- Ranking
- Accessing previous/next row values (lag/lead)
- Moving averages

## Proposed Syntax

Uses `>>` consistently with the rest of patni:

```python
from patni.ibis import (
    window, row_number, rank, dense_rank, lag, lead, ntile,
    mutate, select, filter, it
)

# Define a window specification
w = window(partition_by=it.category, order_by=it.date)

result = (
    t
    >> mutate(
        row_num=row_number(w),
        ranking=rank(w),
        prev_amount=lag(it.amount, 1, w),
        next_amount=lead(it.amount, 1, w),
        running_total=it.amount.sum() >> w,
        moving_avg=it.amount.mean() >> w,
    )
)()
```

## Syntax Patterns

### Dedicated window functions
```python
row_number(w)           # Row number within partition
rank(w)                 # Rank with gaps for ties
dense_rank(w)           # Rank without gaps
lag(it.col, n, w)       # Value from n rows before
lead(it.col, n, w)      # Value from n rows after
ntile(n, w)             # Divide into n buckets
```

### Aggregations over windows
```python
# Use w(expr) syntax since >> conflicts with Ibis's bitwise operators
w(it.amount.sum())     # Running sum
w(it.amount.mean())    # Running average
w(it.amount.min())     # Running min
w(it.amount.max())     # Running max
w(it.amount.count())   # Running count
```

**Note:** We use `w(expr)` instead of `expr >> w` because Ibis expressions
have their own `__rshift__` for bitwise operations which takes precedence.

### Window with frame specification
```python
# Rolling window: current row and 2 preceding
w_rolling = window(
    partition_by=it.category,
    order_by=it.date,
    rows_between=(-2, 0)
)

# Unbounded preceding to current
w_cumulative = window(
    order_by=it.date,
    rows_between=(None, 0)  # None = unbounded
)
```

## Implementation

### WindowSpec class

```python
class WindowSpec:
    """Represents a window specification for window functions."""

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
        import ibis

        kwargs = {}
        if self.partition_by is not None:
            kwargs['group_by'] = self.partition_by
        if self.order_by is not None:
            kwargs['order_by'] = self.order_by
        if self.rows_between is not None:
            kwargs['preceding'] = abs(self.rows_between[0]) if self.rows_between[0] else None
            kwargs['following'] = self.rows_between[1] if self.rows_between[1] else 0

        return ibis.window(**kwargs)

    def __rrshift__(self, expr):
        """Apply window to expression: expr >> window_spec"""
        return expr.over(self._to_ibis())
```

### Window function helpers

```python
def window(partition_by=None, order_by=None, rows_between=None):
    """Create a window specification."""
    return WindowSpec(partition_by, order_by, rows_between)


def row_number(window_spec):
    """Row number within the window partition."""
    import ibis
    return ibis.row_number().over(window_spec._to_ibis())


def rank(window_spec):
    """Rank with gaps for ties."""
    import ibis
    return ibis.rank().over(window_spec._to_ibis())


def dense_rank(window_spec):
    """Rank without gaps for ties."""
    import ibis
    return ibis.dense_rank().over(window_spec._to_ibis())


def lag(column, offset, window_spec, default=None):
    """Access value from previous row."""
    return column.lag(offset, default=default).over(window_spec._to_ibis())


def lead(column, offset, window_spec, default=None):
    """Access value from next row."""
    return column.lead(offset, default=default).over(window_spec._to_ibis())


def ntile(n, window_spec):
    """Divide rows into n roughly equal buckets."""
    import ibis
    return ibis.ntile(n).over(window_spec._to_ibis())
```

## Test Cases

1. `test_row_number` - Basic row numbering within partitions
2. `test_rank_with_ties` - Rank and dense_rank with duplicate values
3. `test_lag_lead` - Accessing previous/next row values
4. `test_running_total` - Cumulative sum with >> syntax
5. `test_moving_average` - Rolling average with frame spec
6. `test_ntile` - Bucket distribution
7. `test_reusable_window` - Same window spec for multiple columns
8. `test_multiple_partitions` - Complex partitioning

## Files Modified

- `patni/ibis.py` - Add WindowSpec and window functions
- `test_ibis.py` - Add TestWindowFunctions class
