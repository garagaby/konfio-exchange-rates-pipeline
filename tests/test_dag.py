import pytest

from src.dag import PipelineDAG


def test_dag_resolves_dependencies_deterministically():
    dag = PipelineDAG()
    dag.add_task("model", lambda _: "model", ("transform",))
    dag.add_task("transform", lambda _: "transform", ("extract",))
    dag.add_task("extract", lambda _: "extract")
    assert dag.execution_order() == ("extract", "transform", "model")
    assert dag.run()["model"] == "model"


def test_dag_rejects_cycles():
    dag = PipelineDAG()
    dag.add_task("a", lambda _: None, ("b",))
    dag.add_task("b", lambda _: None, ("a",))
    with pytest.raises(ValueError, match="cycle"):
        dag.execution_order()
