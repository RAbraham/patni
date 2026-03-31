from patni import do, it, el, map, filter, reduce


def test_single_function():
    square = lambda x: x * x
    result = 3 >> do(square)
    assert result == 9


def test_placeholder_argument():
    subtract = lambda x, y: x - y
    result = 9 >> do(subtract, it, 2)
    assert result == 7


def test_chained_operations():
    square = lambda x: x * x
    subtract = lambda x, y: x - y
    result = 3 >> do(square) >> do(subtract, it, 2)
    assert result == 7


def test_intermediate_variable():
    square = lambda x: x * x
    subtract = lambda x, y: x - y
    sub_tree = 3 >> do(square)
    result = sub_tree >> do(subtract, it, 2)
    assert result == 7


def test_composed_pipeline():
    square = lambda x: x * x
    subtract = lambda x, y: x - y
    pipeline = do(square) >> do(subtract, it, 2)
    result = 3 >> pipeline
    assert result == 7


def test_placeholder_getitem():
    """Placeholder supports item access."""
    result = {'key': 'value'} >> do(it['key'])
    assert result == 'value'


def test_placeholder_list_index():
    """Placeholder supports list indexing."""
    result = [1, 2, 3] >> do(it[1])
    assert result == 2


def test_placeholder_getattr():
    """Placeholder supports attribute access."""
    class Obj:
        name = 'test'
    result = Obj() >> do(it.name)
    assert result == 'test'


def test_placeholder_method_call():
    """Placeholder supports method calls."""
    result = 'hello' >> do(it.upper())
    assert result == 'HELLO'


def test_placeholder_chained_method():
    """Placeholder supports chained method calls."""
    result = '  hello  ' >> do(it.strip().upper())
    assert result == 'HELLO'


def test_placeholder_arithmetic():
    """Placeholder supports arithmetic operations."""
    result = 5 >> do(it * 2 + 1)
    assert result == 11


def test_placeholder_comparison():
    """Placeholder supports comparison operations."""
    result = 5 >> do(it > 3)
    assert result is True


def test_placeholder_complex_expression():
    """Placeholder supports complex expressions."""
    data = {'items': [1, 2, 3]}
    result = data >> do(it['items'][1])
    assert result == 2


def test_map_with_lambda():
    """map applies function to each element."""
    result = [1, 2, 3] >> map(lambda x: x * 2)
    assert result == [2, 4, 6]


def test_map_with_placeholder():
    """map works with placeholder expressions."""
    result = [1, 2, 3] >> map(el * 2)
    assert result == [2, 4, 6]


def test_filter_with_lambda():
    """filter keeps elements matching predicate."""
    result = [1, 2, 3, 4, 5] >> filter(lambda x: x > 2)
    assert result == [3, 4, 5]


def test_filter_with_placeholder():
    """filter works with placeholder expressions."""
    result = [1, 2, 3, 4, 5] >> filter(el > 2)
    assert result == [3, 4, 5]


def test_reduce_with_lambda():
    """reduce combines elements."""
    result = [1, 2, 3, 4] >> reduce(lambda a, b: a + b)
    assert result == 10


def test_reduce_with_initial():
    """reduce with initial value."""
    result = [1, 2, 3, 4] >> reduce(lambda a, b: a + b, 10)
    assert result == 20


def test_chained_map_filter():
    """map and filter can be chained."""
    result = [1, 2, 3, 4, 5] >> map(el * 2) >> filter(el > 5)
    assert result == [6, 8, 10]


def test_map_filter_reduce():
    """Full pipeline with map, filter, reduce."""
    result = [1, 2, 3, 4, 5] >> map(el * 2) >> filter(el > 5) >> reduce(lambda a, b: a + b)
    assert result == 24  # 6 + 8 + 10
