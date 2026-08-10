"""Small dependency-aware DAG runner for the pipeline stages."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable


LOGGER = logging.getLogger(__name__)
TaskAction = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class DAGTask:
    name: str
    dependencies: tuple[str, ...]
    action: TaskAction


class PipelineDAG:
    """Execute named tasks once, in dependency order, with stage logging."""

    def __init__(self) -> None:
        self._tasks: dict[str, DAGTask] = {}

    def add_task(self, name: str, action: TaskAction, dependencies: tuple[str, ...] = ()) -> None:
        if name in self._tasks:
            raise ValueError(f"Duplicate DAG task: {name}")
        self._tasks[name] = DAGTask(name, dependencies, action)

    def execution_order(self) -> tuple[str, ...]:
        """Return a deterministic topological order and reject invalid graphs."""
        state: dict[str, int] = {}
        order: list[str] = []

        def visit(name: str) -> None:
            if name not in self._tasks:
                raise ValueError(f"DAG task dependency is not defined: {name}")
            if state.get(name) == 1:
                raise ValueError(f"DAG cycle detected at task: {name}")
            if state.get(name) == 2:
                return
            state[name] = 1
            for dependency in self._tasks[name].dependencies:
                visit(dependency)
            state[name] = 2
            order.append(name)

        for name in self._tasks:
            visit(name)
        return tuple(order)

    def run(self) -> dict[str, Any]:
        context: dict[str, Any] = {}
        for name in self.execution_order():
            task = self._tasks[name]
            started = perf_counter()
            LOGGER.info("DAG stage started name=%s dependencies=%s", name, ",".join(task.dependencies) or "none")
            try:
                context[name] = task.action(context)
            except Exception:
                LOGGER.exception("DAG stage failed name=%s elapsed_seconds=%.3f", name, perf_counter() - started)
                raise
            LOGGER.info("DAG stage completed name=%s elapsed_seconds=%.3f", name, perf_counter() - started)
        return context
