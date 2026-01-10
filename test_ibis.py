"""Tests for patni.ibis integration."""

import pytest

# Skip all tests if ibis is not installed
ibis = pytest.importorskip("ibis")

from patni.ibis import (
    it, select, filter, group_by, agg, aggregate, order_by, limit,
    mutate, distinct, join, head, drop, rename, IbisDeferred
)


@pytest.fixture
def con():
    """Create an in-memory DuckDB connection."""
    return ibis.duckdb.connect()


@pytest.fixture
def users_table(con):
    """Create a users table with test data."""
    import pandas as pd

    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 22],
        "city": ["NYC", "LA", "NYC", "LA", "NYC"],
        "salary": [50000, 60000, 70000, 55000, 45000],
    })
    con.create_table("users", df)
    return con.table("users")


@pytest.fixture
def orders_table(con):
    """Create an orders table with test data."""
    import pandas as pd

    df = pd.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "user_id": [1, 1, 2, 3, 4],
        "amount": [100, 200, 150, 300, 250],
    })
    con.create_table("orders", df)
    return con.table("orders")


class TestBasicOperations:
    """Test basic pipe operations."""

    def test_select(self, users_table):
        """Test select operation."""
        result = (users_table >> select(it.name, it.age))()
        assert list(result.columns) == ["name", "age"]
        assert len(result) == 5

    def test_filter(self, users_table):
        """Test filter operation."""
        result = (users_table >> filter(it.age > 25))()
        assert len(result) == 3
        assert all(result["age"] > 25)

    def test_limit(self, users_table):
        """Test limit operation."""
        result = (users_table >> limit(2))()
        assert len(result) == 2

    def test_head(self, users_table):
        """Test head operation (alias for limit)."""
        result = (users_table >> head(3))()
        assert len(result) == 3

    def test_order_by(self, users_table):
        """Test order_by operation."""
        result = (users_table >> order_by(it.age))()
        ages = list(result["age"])
        assert ages == sorted(ages)

    def test_order_by_desc(self, users_table):
        """Test order_by descending."""
        result = (users_table >> order_by(it.age.desc()))()
        ages = list(result["age"])
        assert ages == sorted(ages, reverse=True)


class TestChainedOperations:
    """Test chaining multiple operations."""

    def test_filter_then_select(self, users_table):
        """Test filter followed by select."""
        result = (
            users_table
            >> filter(it.age > 25)
            >> select(it.name, it.age)
        )()
        assert list(result.columns) == ["name", "age"]
        assert len(result) == 3

    def test_complex_chain(self, users_table):
        """Test complex chain of operations."""
        result = (
            users_table
            >> filter(it.age >= 25)
            >> select(it.name, it.age, it.salary)
            >> order_by(it.salary.desc())
            >> limit(2)
        )()
        assert len(result) == 2
        assert list(result.columns) == ["name", "age", "salary"]


class TestAggregations:
    """Test group by and aggregation operations."""

    def test_group_by_agg(self, users_table):
        """Test group_by with aggregation."""
        result = (
            users_table
            >> group_by(it.city)
            >> agg(count=it.id.count(), avg_age=it.age.mean())
        )()
        assert len(result) == 2  # NYC and LA
        assert "count" in result.columns
        assert "avg_age" in result.columns

    def test_group_by_agg_order(self, users_table):
        """Test group_by with aggregation and ordering."""
        result = (
            users_table
            >> group_by(it.city)
            >> agg(total_salary=it.salary.sum())
            >> order_by(it.total_salary.desc())
        )()
        salaries = list(result["total_salary"])
        assert salaries == sorted(salaries, reverse=True)


class TestMutations:
    """Test mutate operations."""

    def test_mutate_new_column(self, users_table):
        """Test adding a new column."""
        result = (
            users_table
            >> mutate(age_plus_10=it.age + 10)
            >> select(it.name, it.age, it.age_plus_10)
        )()
        assert "age_plus_10" in result.columns
        assert list(result["age_plus_10"]) == [35, 40, 45, 38, 32]

    def test_mutate_computed_column(self, users_table):
        """Test adding a computed column."""
        result = (
            users_table
            >> mutate(annual_bonus=it.salary * 0.1)
            >> select(it.name, it.salary, it.annual_bonus)
        )()
        assert "annual_bonus" in result.columns


class TestDistinct:
    """Test distinct operations."""

    def test_distinct(self, users_table):
        """Test distinct values."""
        result = (
            users_table
            >> select(it.city)
            >> distinct()
        )()
        assert len(result) == 2  # NYC and LA


class TestJoins:
    """Test join operations."""

    def test_inner_join(self, users_table, orders_table):
        """Test inner join."""
        result = (
            users_table
            >> join(orders_table, it.id == orders_table.user_id)
            >> select(it.name, orders_table.amount)
        )()
        assert len(result) == 5  # 5 orders
        assert "name" in result.columns
        assert "amount" in result.columns


class TestDeferredExecution:
    """Test lazy execution behavior."""

    def test_returns_deferred(self, users_table):
        """Verify that operations return IbisDeferred."""
        expr = users_table >> filter(it.age > 25)
        assert isinstance(expr, IbisDeferred)

    def test_deferred_not_executed_until_called(self, users_table):
        """Verify lazy evaluation."""
        expr = users_table >> filter(it.age > 25) >> limit(10)
        assert isinstance(expr, IbisDeferred)
        # Only executes when called
        result = expr()
        assert len(result) == 3

    def test_expr_property(self, users_table):
        """Test access to underlying Ibis expression."""
        deferred = users_table >> filter(it.age > 25)
        assert hasattr(deferred, 'expr')
        # The expr should be an Ibis expression, not executed
        assert hasattr(deferred.expr, 'execute')

    def test_caching(self, users_table):
        """Test that results are cached."""
        deferred = users_table >> filter(it.age > 25)
        result1 = deferred()
        result2 = deferred()
        # Should return same cached result
        assert result1 is result2


class TestComposedPipelines:
    """Test composing pipelines without initial table."""

    def test_compose_operations(self, users_table):
        """Test composing operations into a reusable pipeline."""
        # Create a reusable pipeline
        young_and_sorted = filter(it.age < 30) >> order_by(it.age)

        result = (users_table >> young_and_sorted)()
        assert len(result) == 3  # Alice (25), Diana (28), Eve (22)
        ages = list(result["age"])
        assert ages == sorted(ages)

    def test_reuse_pipeline(self, users_table, con):
        """Test reusing a pipeline on different tables."""
        import pandas as pd

        # Create another table
        df2 = pd.DataFrame({
            "id": [1, 2],
            "name": ["Frank", "Grace"],
            "age": [40, 20],
            "city": ["SF", "SF"],
            "salary": [80000, 40000],
        })
        con.create_table("users2", df2)
        users2 = con.table("users2")

        # Reusable pipeline
        adults = filter(it.age >= 25) >> select(it.name, it.age)

        result1 = (users_table >> adults)()
        result2 = (users2 >> adults)()

        assert len(result1) == 4  # Alice(25), Bob(30), Charlie(35), Diana(28)
        assert len(result2) == 1  # Frank(40)


class TestComplexMultiJoin:
    """Test complex multi-join patterns matching Ibis's _ context behavior."""

    @pytest.fixture
    def t1(self, con):
        """Create t1 table with x, y columns."""
        import pandas as pd

        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5, 6],
            "y": [10, 20, 30, 40, 50, 60],
        })
        con.create_table("t1", df)
        return con.table("t1")

    @pytest.fixture
    def t2(self, con):
        """Create t2 table with x, z columns."""
        import pandas as pd

        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5, 6],
            "z": [100, 200, 300, 400, 500, 600],
        })
        con.create_table("t2", df)
        return con.table("t2")

    def test_join_mutate_groupby_agg(self, t1, t2):
        """Test join followed by mutate, group_by, and aggregate."""
        # Deferred expression for xmod
        xmod = it.x % 3

        result = (
            t1
            >> join(t2, it.x == t2.x)
            >> mutate(xmod=xmod)
            >> group_by(it.xmod)
            >> agg(ymax=it.y.max(), zmax=it.z.max())
            >> order_by(it.xmod)
        )()

        assert len(result) == 3  # xmod values: 0, 1, 2
        assert "xmod" in result.columns
        assert "ymax" in result.columns
        assert "zmax" in result.columns

    def test_aggregate_alias(self, t1, t2):
        """Test that aggregate works as alias for agg."""
        xmod = it.x % 2

        result = (
            t1
            >> join(t2, it.x == t2.x)
            >> mutate(xmod=xmod)
            >> group_by(it.xmod)
            >> aggregate(total_y=it.y.sum(), total_z=it.z.sum())
        )()

        assert len(result) == 2  # xmod values: 0, 1
        assert "total_y" in result.columns
        assert "total_z" in result.columns

    def test_filter_after_agg(self, t1, t2):
        """Test filtering after aggregation."""
        result = (
            t1
            >> join(t2, it.x == t2.x)
            >> mutate(xmod=it.x % 3)
            >> group_by(it.xmod)
            >> agg(ymax=it.y.max(), zmax=it.z.max())
            >> filter(it.ymax > 30)
        )()

        # Only groups where ymax > 30
        assert all(result["ymax"] > 30)

    def test_multiple_joins(self, t1, t2, con):
        """Test multiple sequential joins."""
        import pandas as pd

        # Create t3 table
        df3 = pd.DataFrame({
            "x": [1, 2, 3, 4, 5, 6],
            "w": [1000, 2000, 3000, 4000, 5000, 6000],
        })
        con.create_table("t3", df3)
        t3 = con.table("t3")

        result = (
            t1
            >> join(t2, it.x == t2.x)
            >> join(t3, it.x == t3.x)
            >> select(it.x, it.y, t2.z, t3.w)
        )()

        assert len(result) == 6
        assert "x" in result.columns
        assert "y" in result.columns
        assert "z" in result.columns
        assert "w" in result.columns

    def test_full_complex_pipeline(self, t1, t2):
        """Test full complex pipeline similar to user's example."""
        # Define helper function
        def modf(t):
            return t.x % 3

        # Deferred expressions
        xmod = it.x % 3
        ymax = it.y.max()
        zmax = it.z.max()

        result = (
            t1
            >> join(t2, it.x == t2.x)
            >> mutate(xmod=xmod)
            >> group_by(it.xmod)
            >> agg(ymax=ymax, zmax=zmax)
            >> filter(it.ymax >= it.zmax / 10)  # Adjusted filter for test data
            >> order_by(it.xmod)
        )()

        assert "xmod" in result.columns
        assert "ymax" in result.columns
        assert "zmax" in result.columns

    def test_deferred_expressions_outside(self, t1):
        """Test using deferred expressions defined outside the pipeline."""
        # Define expressions outside
        double_x = it.x * 2
        x_squared = it.x * it.x

        result = (
            t1
            >> mutate(double_x=double_x, x_squared=x_squared)
            >> select(it.x, it.double_x, it.x_squared)
        )()

        assert list(result["double_x"]) == [2, 4, 6, 8, 10, 12]
        assert list(result["x_squared"]) == [1, 4, 9, 16, 25, 36]


class TestSubTree:
    """Test sub_tree concept - composing pipelines without initial table."""

    @pytest.fixture
    def t1(self, con):
        """Create t1 table."""
        import pandas as pd

        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5, 6],
            "y": [10, 20, 30, 40, 50, 60],
        })
        con.create_table("subtree_t1", df)
        return con.table("subtree_t1")

    def test_simple_subtree_composition(self, t1):
        """Test composing two operations into a sub_tree."""
        subtree1 = filter(it.x > 2)
        subtree2 = select(it.x, it.y)
        pipeline = subtree1 >> subtree2

        result = (t1 >> pipeline)()
        assert len(result) == 4  # x > 2 means 3,4,5,6
        assert list(result.columns) == ["x", "y"]

    def test_complex_subtree(self, t1):
        """Test complex sub_tree with multiple operations."""
        transform = mutate(x2=it.x * 2) >> filter(it.x2 > 6) >> select(it.x, it.x2)

        result = (t1 >> transform)()
        assert list(result["x2"]) == [8, 10, 12]  # x=4,5,6 -> x2=8,10,12

    def test_reuse_subtree_on_different_tables(self, t1, con):
        """Test reusing a sub_tree on different tables."""
        import pandas as pd

        df2 = pd.DataFrame({"x": [10, 20], "y": [100, 200]})
        con.create_table("subtree_t2", df2)
        t2 = con.table("subtree_t2")

        # Define reusable pipeline
        double_and_filter = mutate(doubled=it.x * 2) >> filter(it.doubled > 10)

        result_t1 = (t1 >> double_and_filter)()
        result_t2 = (t2 >> double_and_filter)()

        assert len(result_t1) == 1   # only x=6 -> doubled=12 > 10
        assert len(result_t2) == 2   # both 10,20 -> 20,40 > 10

    def test_subtree_with_aggregation(self, t1):
        """Test sub_tree with group_by and aggregation."""
        agg_pipeline = (
            mutate(group=it.x % 2)
            >> group_by(it.group)
            >> agg(total=it.y.sum())
        )

        result = (t1 >> agg_pipeline)()
        assert len(result) == 2  # odd/even groups

    def test_subtree_stored_and_extended(self, t1):
        """Test storing a sub_tree and extending it later."""
        # Base pipeline
        base = filter(it.x > 1) >> mutate(doubled=it.x * 2)

        # Extended pipelines
        with_select = base >> select(it.x, it.doubled)
        with_filter = base >> filter(it.doubled > 6)

        result1 = (t1 >> with_select)()
        result2 = (t1 >> with_filter)()

        assert len(result1) == 5  # x > 1: 2,3,4,5,6
        assert list(result1.columns) == ["x", "doubled"]

        assert len(result2) == 3  # doubled > 6: x=4,5,6 (8,10,12)

    def test_subtree_with_order(self, t1):
        """Test sub_tree with ordering."""
        pipeline = filter(it.x > 2) >> order_by(it.y.desc()) >> limit(2)

        result = (t1 >> pipeline)()
        assert len(result) == 2
        assert list(result["x"]) == [6, 5]  # descending by y
