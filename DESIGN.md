
# Examples
```python
from patni import do, it
square = lambda x: x*x
subtract = lambda x, y: x - y

y = 3 >> do(square) >> do(subtract, it, 2)
assert y == 7

sub_tree = 3 >> do(square)
y = sub_tree >> do(subtract, it, 2)
assert y == 7
```
