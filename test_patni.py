from patni import do, it

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
