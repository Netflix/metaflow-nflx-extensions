import json
import os
from types import SimpleNamespace

from metaflow import metaflow_config

from metaflow_extensions.nflx.plugins.functions.factory import (
    FunctionTypeConfig,
    create_function_type,
)


class _Task:
    pathspec = "Flow/1/step/task"
    code = SimpleNamespace(path="/tmp/code.tar")
    metadata_dict = {"conda_env_id": json.dumps(["test", "1", "linux-64"])}
    artifacts = []
    successful = True


def _export_to(monkeypatch, root, name):
    monkeypatch.setattr(metaflow_config, "DEFAULT_DATASTORE", "local")
    monkeypatch.setattr(metaflow_config, "DEFAULT_FUNCTIONS_LOCAL_ROOT", str(root))

    Function, decorator = create_function_type(
        FunctionTypeConfig(
            name=name,
            param_validators=[lambda type_hint: type_hint is str],
            return_validator=lambda type_hint: type_hint is str,
            param_count=1,
        )
    )

    @decorator
    def handler(data: str) -> str:
        return data

    return Function(handler, task=_Task()).spec


def test_local_export_creates_shard_directories(monkeypatch, tmp_path):
    spec = _export_to(monkeypatch, tmp_path, "export_shard_dirs_function")

    assert os.path.isfile(spec.code_package)
    assert os.path.isfile(spec.reference)
    assert os.path.basename(os.path.dirname(spec.code_package)) == spec.package_uuid[:2]
    assert os.path.basename(os.path.dirname(spec.reference)) == spec.uuid[:2]


def test_local_export_reuses_an_existing_shard_directory(monkeypatch, tmp_path):
    first = _export_to(monkeypatch, tmp_path, "export_shard_reuse_function")
    second = _export_to(monkeypatch, tmp_path, "export_shard_reuse_function")

    assert os.path.dirname(second.code_package) == os.path.dirname(first.code_package)
    assert os.path.isfile(second.code_package)
    assert os.path.isfile(second.reference)
