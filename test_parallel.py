import time
from patni.parallel import do, it, Deferred

def test_lazy_evaluation():
    """Computation doesn't run until called."""
    call_count = 0
    def tracked_square(x):
        nonlocal call_count
        call_count += 1
        return x * x

    pipeline = 3 >> do(tracked_square)
    assert isinstance(pipeline, Deferred)
    assert call_count == 0  # Not yet executed
    result = pipeline()
    assert call_count == 1
    assert result == 9

def test_single_function():
    """Basic lazy pipeline with single function."""
    square = lambda x: x * x
    result = 3 >> do(square)
    assert result() == 9

def test_placeholder_argument():
    """Lazy pipeline with placeholder argument."""
    subtract = lambda x, y: x - y
    result = 9 >> do(subtract, it, 2)
    assert result() == 7

def test_chained_operations():
    """Lazy pipeline with chained operations."""
    square = lambda x: x * x
    subtract = lambda x, y: x - y
    result = 3 >> do(square) >> do(subtract, it, 2)
    assert result() == 7

def test_composed_pipeline():
    """Compose pipelines without initial value."""
    square = lambda x: x * x
    subtract = lambda x, y: x - y
    pipeline = do(square) >> do(subtract, it, 2)
    result = 3 >> pipeline
    assert result() == 7

def test_parallel_branches():
    """Independent branches execute in parallel."""
    square = lambda x: x * x
    add = lambda x, y: x + y

    a = 3 >> do(square)  # Deferred: 9
    b = 4 >> do(square)  # Deferred: 16

    # Combine branches using tuple
    result = (a, b) >> do(lambda t: add(t[0], t[1]))
    assert result() == 25

def test_parallel_execution_timing():
    """Verify parallel execution is faster than sequential."""
    def slow_func(x):
        time.sleep(0.1)
        return x * 2

    # Create two independent branches
    a = 3 >> do(slow_func)
    b = 4 >> do(slow_func)

    start = time.time()
    result = (a, b) >> do(lambda t: t[0] + t[1])
    value = result()
    elapsed = time.time() - start

    assert value == 14  # (3*2) + (4*2)
    # Parallel should take ~0.1s, sequential would take ~0.2s
    assert elapsed < 0.18, f"Expected parallel execution, took {elapsed:.2f}s"

def test_deferred_dag():
    """Full DAG with sync point."""
    square = lambda x: x * x
    add = lambda a, b: a + b
    increment = lambda x: x + 1

    subtree1 = do(square)           # x -> x^2
    subtree2 = do(increment)        # x -> x+1

    a = 3 >> subtree1 >> subtree2   # 3 -> 9 -> 10
    b = 4 >> subtree1               # 4 -> 16

    c = (a, b) >> do(lambda t: add(t[0], t[1]))
    assert c() == 26

def test_deferred_as_argument():
    """Deferred objects can be passed as arguments to do()."""
    square = lambda x: x * x
    multiply = lambda x, y: x * y

    a = 3 >> do(square)  # Deferred: 9
    result = 2 >> do(multiply, it, a)  # 2 * 9 = 18
    assert result() == 18

def test_multiple_deferred_arguments():
    """Multiple Deferred arguments are resolved in parallel."""
    def slow_square(x):
        time.sleep(0.1)
        return x * x

    a = 3 >> do(slow_square)  # Deferred: 9
    b = 4 >> do(slow_square)  # Deferred: 16

    start = time.time()
    result = 0 >> do(lambda _, x, y: x + y, it, a, b)
    value = result()
    elapsed = time.time() - start

    assert value == 25  # 9 + 16
    # Both a and b should resolve in parallel
    assert elapsed < 0.18, f"Expected parallel execution, took {elapsed:.2f}s"

def test_caching():
    """Deferred result is cached after first evaluation."""
    call_count = 0
    def tracked_func(x):
        nonlocal call_count
        call_count += 1
        return x * 2

    deferred = 5 >> do(tracked_func)
    assert deferred() == 10
    assert deferred() == 10  # Second call
    assert call_count == 1  # Function only called once
