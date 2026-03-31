# Patni Roadmap

Feature suggestions for future development.

## patni (sequential/eager)

### Debugging & Observability

- [ ] `tap(func)` - Execute side-effect without changing value
  ```python
  result = 3 >> do(square) >> tap(print) >> do(add, it, 1)
  # prints 9, result = 10
  ```

- [ ] `debug()` - Print intermediate value with label
  ```python
  result = 3 >> do(square) >> debug("after square") >> do(add, it, 1)
  # prints: [after square] 9
  ```

### Error Handling

- [ ] `catch(handler)` - Handle errors in pipeline
  ```python
  result = data >> do(risky_func) >> catch(lambda e: default_value)
  ```

- [ ] `validate(predicate, error_msg)` - Assert conditions
  ```python
  result = x >> validate(lambda v: v > 0, "must be positive") >> do(sqrt)
  ```

### Control Flow

- [ ] `when(predicate, then_pipe, else_pipe)` - Conditional branching
  ```python
  result = x >> when(lambda v: v > 0, do(sqrt), do(abs) >> do(sqrt))
  ```

---

## patni.parallel

### Async Support

- [ ] `async` execution - Asyncio integration
  ```python
  result = await deferred.async_call()
  # or
  result = await deferred
  ```

### Resource Management

- [ ] `max_workers` configuration - Limit thread pool size
  ```python
  from patni.parallel import configure
  configure(max_workers=4)
  ```

- [ ] `timeout(seconds)` - Fail if computation exceeds time limit
  ```python
  result = (slow_op >> timeout(5.0))()
  ```

### Reliability

- [ ] `retry(n, backoff)` - Retry failed computations
  ```python
  result = (flaky_op >> retry(3, backoff=1.0))()
  ```

- [ ] `cache()` - Memoize expensive computations
  ```python
  expensive = do(heavy_compute) >> cache()
  ```

### Observability

- [ ] `progress(callback)` - Track execution progress
  ```python
  def on_progress(completed, total):
      print(f"{completed}/{total}")

  result = (big_dag >> progress(on_progress))()
  ```

- [ ] `visualize()` - Show DAG structure
  ```python
  from patni.parallel import do, it, visualize

  # Define parallel subtrees
  square = lambda x: x * x
  double = lambda x: x * 2
  add = lambda x, y: x + y

  # Branch A: 3 -> square -> 9
  branch_a = 3 >> do(square)

  # Branch B: 4 -> double -> 8
  branch_b = 4 >> do(double)

  # Merge: (9, 8) -> add -> 17
  result = (branch_a, branch_b) >> do(lambda t: add(t[0], t[1]))

  # Visualize the DAG
  result.visualize()
  ```

  **Output (ASCII):**
  ```
       3           4
       │           │
       ▼           ▼
   ┌───────┐   ┌───────┐
   │square │   │double │
   └───┬───┘   └───┬───┘
       │           │
       ▼           ▼
       9           8
        \         /
         \       /
          ▼     ▼
        ┌───────┐
        │  add  │
        └───┬───┘
            │
            ▼
           17
  ```

  **Features:**
  - Identify parallel branches (can run concurrently)
  - Show data flow and dependencies
  - Export to Graphviz DOT format for complex DAGs

---

## patni.ibis

### Window Functions

- [ ] `window()` - Window function support
  ```python
  result = (
      t
      >> window(
          row_num=it.row_number(),
          running_total=it.amount.sum(),
          over=partition_by(it.category).order_by(it.date)
      )
  )()
  ```

- [ ] Individual window functions
  - `row_number()`
  - `rank()`, `dense_rank()`
  - `lag(col, n)`, `lead(col, n)`
  - `first()`, `last()`
  - `ntile(n)`

### Conditional Expressions

- [ ] `case()` - Case/when expressions
  ```python
  result = (
      t
      >> mutate(
          category=case(
              (it.amount > 100, "high"),
              (it.amount > 50, "medium"),
              else_="low"
          )
      )
  )()
  ```

### Set Operations

- [ ] `union(other)` - Combine tables
  ```python
  result = (t1 >> union(t2))()
  ```

- [ ] `intersect(other)` - Common rows
  ```python
  result = (t1 >> intersect(t2))()
  ```

- [ ] `except_(other)` - Rows in t1 but not t2
  ```python
  result = (t1 >> except_(t2))()
  ```

### Join Shortcuts

- [ ] `left_join(right, predicate)`
- [ ] `right_join(right, predicate)`
- [ ] `outer_join(right, predicate)`
- [ ] `cross_join(right)`
- [ ] `semi_join(right, predicate)` - Rows in left that have match in right
- [ ] `anti_join(right, predicate)` - Rows in left that have no match in right

### Sampling

- [ ] `sample(n)` - Random n rows
  ```python
  result = (t >> sample(100))()
  ```

- [ ] `sample(frac=0.1)` - Random fraction of rows
  ```python
  result = (t >> sample(frac=0.1))()
  ```

### Query Inspection

- [ ] `explain()` - Show query plan without executing
  ```python
  plan = (t >> filter(it.x > 10) >> select(it.y)).explain()
  print(plan)
  ```

- [ ] `to_sql()` - Get generated SQL
  ```python
  sql = (t >> filter(it.x > 10)).to_sql()
  ```

### Raw SQL

- [ ] `sql(query)` - Raw SQL escape hatch
  ```python
  result = (t >> sql("SELECT * FROM {t} WHERE x > 10"))()
  ```

### Common Table Expressions

- [ ] `cte(name)` - Create named CTE
  ```python
  base = t >> filter(it.x > 10) >> cte("filtered")
  result = (base >> join(base, it.id == base.parent_id))()
  ```

### Null Handling

- [ ] `coalesce(*cols)` - First non-null value
  ```python
  result = (t >> mutate(value=coalesce(it.a, it.b, 0)))()
  ```

- [ ] `fill_null(col, value)` - Replace nulls
  ```python
  result = (t >> fill_null(it.name, "unknown"))()
  ```

---

## Cross-cutting Features

### Pipeline Representation

- [ ] `__repr__` / `__str__` - Pretty print pipeline structure
  ```python
  pipeline = filter(it.x > 10) >> select(it.y) >> limit(5)
  print(pipeline)
  # Output: filter(x > 10) >> select(y) >> limit(5)
  ```

### Serialization

- [ ] `to_json()` / `from_json()` - Serialize pipelines
  ```python
  pipeline = filter(it.x > 10) >> select(it.y)
  json_str = pipeline.to_json()

  loaded = Pipeline.from_json(json_str)
  ```

### Error Tracing

- [ ] Pipeline stack traces - Show which step failed
  ```
  PipelineError: Error in step 3 (filter)
    Pipeline: select(x, y) >> mutate(z=x*2) >> filter(z > 100)
                                               ^^^^^^^^^^^^^^^^
    Caused by: TypeError: '>' not supported between 'str' and 'int'
  ```

### Testing Utilities

- [ ] `assert_pipeline_equals(p1, p2)` - Compare pipeline structures
- [ ] `mock_deferred(value)` - Create test doubles

---

## Priority Suggestions

### High Priority (Most Useful)
1. `tap()` and `debug()` - Essential for development
2. Window functions for ibis - Common SQL need
3. Join shortcuts - Cleaner API
4. `explain()` / `to_sql()` - Debugging queries

### Medium Priority
1. `catch()` error handling
2. `case()` expressions
3. Set operations (union, intersect)
4. Async support for parallel

### Lower Priority (Nice to Have)
1. Serialization
2. Visualization
3. Progress tracking
4. CTE support
