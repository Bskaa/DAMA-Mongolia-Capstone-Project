"""Minimal stand-ins for the Airflow names the DAG files import.

Airflow is not installed in the unit-test environment. The project's own
airflow/ folder is importable as a namespace package, so we probe a real
Airflow module rather than the top-level name before installing stubs.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
CREATED_DAGS: list["_DAG"] = []


class _DAG:
    def __init__(self, dag_id=None, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks: list[_Operator] = []
        CREATED_DAGS.append(self)

    def __enter__(self):
        _DAG.current = self
        return self

    def __exit__(self, *exc):
        _DAG.current = None
        return False


_DAG.current = None


class _Operator:
    def __init__(self, task_id=None, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.upstream: set[str] = set()
        if _DAG.current is not None:
            _DAG.current.tasks.append(self)

    def _link(self, other):
        for item in other if isinstance(other, (list, tuple)) else [other]:
            item.upstream.add(self.task_id)
        return other

    def __rshift__(self, other):
        return self._link(other)

    def __rrshift__(self, other):
        for item in other if isinstance(other, (list, tuple)) else [other]:
            item._link(self)
        return self


class _Param:
    def __init__(self, default, **kwargs):
        self.default = default


class _TriggerRule:
    ALL_DONE = "all_done"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"


def install() -> None:
    try:
        import airflow.sdk  # noqa: F401
        return
    except ImportError:
        pass
    names = [
        "airflow", "airflow.exceptions", "airflow.sdk", "airflow.task", "airflow.task.trigger_rule",
        "airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators",
        "airflow.providers.standard.operators.python",
    ]
    modules = {name: types.ModuleType(name) for name in names}
    modules["airflow.exceptions"].AirflowException = type("AirflowException", (Exception,), {})
    modules["airflow.exceptions"].AirflowSkipException = type("AirflowSkipException", (Exception,), {})
    modules["airflow.sdk"].DAG = _DAG
    modules["airflow.sdk"].Param = _Param
    modules["airflow.task.trigger_rule"].TriggerRule = _TriggerRule
    modules["airflow.providers.standard.operators.python"].PythonOperator = _Operator
    sys.modules.update(modules)


def load_dag_file(name: str):
    install()
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))  # Airflow puts the dags folder on sys.path
    spec = importlib.util.spec_from_file_location(name, DAGS_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
