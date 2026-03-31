"""Tests for visualize() feature in patni.parallel."""

import pytest
import io
from patni.parallel import do, it, Deferred, Pipeable


# Import visualize when implemented
# from patni.parallel import visualize


class TestVisualizePipeSyntax:
    """Test that visualize works with >> pipe syntax."""

    def test_visualize_returns_deferred(self):
        """visualize() should return the deferred for chaining."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        result = dag >> visualize()

        assert isinstance(result, Deferred)

    def test_visualize_chainable(self):
        """visualize() should be chainable in the middle of a pipeline."""
        from patni.parallel import visualize

        result = (
            3
            >> do(lambda x: x * x)
            >> visualize()
            >> do(lambda x: x + 1)
        )()

        assert result == 10  # 3^2 + 1

    def test_visualize_prints_to_stdout(self, capsys):
        """Default visualize() should print to stdout."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        dag >> visualize()

        captured = capsys.readouterr()
        assert len(captured.out) > 0
        assert "3" in captured.out

    def test_visualize_output_return(self):
        """visualize(output='return') should return string instead of deferred."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        output = dag >> visualize(output="return")

        assert isinstance(output, str)
        assert "3" in output

    def test_visualize_to_file(self, tmp_path):
        """visualize(file=path) should write to file."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        file_path = tmp_path / "dag.txt"

        result = dag >> visualize(file=str(file_path))

        assert isinstance(result, Deferred)  # Still chainable
        assert file_path.exists()
        content = file_path.read_text()
        assert "3" in content

    def test_visualize_to_file_object(self):
        """visualize(file=file_object) should write to file object."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        buffer = io.StringIO()

        dag >> visualize(file=buffer)

        content = buffer.getvalue()
        assert "3" in content


class TestVisualizeFormats:
    """Test different visualization formats."""

    def test_format_ascii(self):
        """Test ASCII format output."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        output = dag >> visualize(format="ascii", output="return")

        assert isinstance(output, str)
        # ASCII uses box drawing or simple characters
        assert any(c in output for c in ['│', '|', '-', '─', '>', '▼', '+'])

    def test_format_dot(self):
        """Test Graphviz DOT format output."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        output = dag >> visualize(format="dot", output="return")

        assert "digraph" in output
        assert "{" in output
        assert "}" in output
        assert "->" in output

    def test_format_simple(self):
        """Test simple text format."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x) >> do(lambda x: x + 1)
        output = dag >> visualize(format="simple", output="return")

        # Simple: "3 -> func1 -> func2"
        assert "->" in output

    def test_invalid_format_raises(self):
        """Invalid format should raise ValueError."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)

        with pytest.raises(ValueError):
            dag >> visualize(format="invalid")


class TestVisualizeParallelBranches:
    """Test visualization of parallel branches."""

    def test_parallel_branches_shown(self):
        """Parallel branches should both appear in output."""
        from patni.parallel import visualize

        branch_a = 3 >> do(lambda x: x * x)
        branch_b = 4 >> do(lambda x: x * 2)
        dag = (branch_a, branch_b) >> do(lambda t: t[0] + t[1])

        output = dag >> visualize(output="return")

        assert "3" in output
        assert "4" in output

    def test_parallel_branches_dot_format(self):
        """DOT format should show parallel structure."""
        from patni.parallel import visualize

        branch_a = 3 >> do(lambda x: x * x)
        branch_b = 4 >> do(lambda x: x * 2)
        dag = (branch_a, branch_b) >> do(lambda t: t[0] + t[1])

        output = dag >> visualize(format="dot", output="return")

        # Should have edges from both branches to merge node
        assert output.count("->") >= 2

    def test_show_parallel_markers(self):
        """show_parallel=True should mark parallel execution."""
        from patni.parallel import visualize

        branch_a = 3 >> do(lambda x: x * x)
        branch_b = 4 >> do(lambda x: x * 2)
        dag = (branch_a, branch_b) >> do(lambda t: t[0] + t[1])

        output = dag >> visualize(show_parallel=True, output="return")

        # Should indicate parallelism
        assert any(s in output.lower() for s in ['parallel', '∥', '||', 'concurrent'])

    def test_complex_dag_multiple_merges(self):
        """Test DAG with multiple merge points."""
        from patni.parallel import visualize

        a = 1 >> do(lambda x: x + 1)
        b = 2 >> do(lambda x: x * 2)
        c = (a, b) >> do(lambda t: t[0] + t[1])
        d = 3 >> do(lambda x: x - 1)
        result = (c, d) >> do(lambda t: t[0] + t[1])

        output = result >> visualize(output="return")

        assert "1" in output
        assert "2" in output
        assert "3" in output


class TestVisualizeDeferredAsArg:
    """Test visualization when Deferred is passed as argument."""

    def test_deferred_arg_shown(self):
        """Deferred passed as argument should appear in visualization."""
        from patni.parallel import visualize

        a = 3 >> do(lambda x: x * x)
        result = 2 >> do(lambda x, y: x * y, it, a)

        output = result >> visualize(output="return")

        assert "2" in output
        assert "3" in output

    def test_multiple_deferred_args(self):
        """Multiple Deferred arguments should all appear."""
        from patni.parallel import visualize

        a = 3 >> do(lambda x: x * x)
        b = 4 >> do(lambda x: x * 2)
        result = 1 >> do(lambda x, y, z: x + y + z, it, a, b)

        output = result >> visualize(output="return")

        assert "1" in output
        assert "3" in output
        assert "4" in output


class TestVisualizePipeable:
    """Test visualization of Pipeable (composed pipeline without value)."""

    def test_pipeable_visualize(self):
        """Composed Pipeable should support visualization."""
        from patni.parallel import visualize

        pipeline = do(lambda x: x * x) >> do(lambda x: x + 1)
        output = pipeline >> visualize(output="return")

        assert isinstance(output, str)
        assert len(output) > 0

    def test_pipeable_visualize_shows_chain(self):
        """Pipeable visualization should show operation chain."""
        from patni.parallel import visualize

        pipeline = do(lambda x: x * x) >> do(lambda x: x + 1) >> do(lambda x: x * 2)
        output = pipeline >> visualize(format="simple", output="return")

        # Should show chain structure
        assert output.count("->") >= 2


class TestVisualizeEdgeCases:
    """Test edge cases."""

    def test_already_executed(self):
        """Visualization should work on already-executed Deferred."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x)
        dag()  # Execute first

        output = dag >> visualize(output="return")
        assert "3" in output

    def test_single_value_no_ops(self):
        """Edge case: Deferred with just a value."""
        from patni.parallel import visualize

        # This is an edge case - might not be common usage
        dag = 3 >> do(lambda x: x)
        output = dag >> visualize(output="return")

        assert "3" in output

    def test_deeply_nested(self):
        """Should handle deeply nested DAGs."""
        from patni.parallel import visualize

        dag = 1 >> do(lambda x: x + 1)
        for _ in range(20):
            dag = dag >> do(lambda x: x + 1)

        output = dag >> visualize(output="return")
        assert isinstance(output, str)

    def test_named_functions(self):
        """Named functions should show their names."""
        from patni.parallel import visualize

        def my_square(x):
            return x * x

        def my_double(x):
            return x * 2

        dag = 3 >> do(my_square) >> do(my_double)
        output = dag >> visualize(output="return")

        assert "my_square" in output
        assert "my_double" in output


class TestVisualizeLabels:
    """Test custom labeling in visualization."""

    def test_custom_label(self):
        """Operations can have custom labels for visualization."""
        from patni.parallel import visualize

        dag = 3 >> do(lambda x: x * x, label="square")
        output = dag >> visualize(output="return")

        assert "square" in output

    def test_custom_labels_in_dot(self):
        """Custom labels should appear in DOT output."""
        from patni.parallel import visualize

        dag = (
            3
            >> do(lambda x: x * x, label="square")
            >> do(lambda x: x + 1, label="increment")
        )
        output = dag >> visualize(format="dot", output="return")

        assert "square" in output
        assert "increment" in output
