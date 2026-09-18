import json
from types import SimpleNamespace

import pytest

from metaflow_extensions.nflx.plugins.functions.core.function import MetaflowFunction
from metaflow_extensions.nflx.plugins.functions.exceptions import (
    MetaflowFunctionException,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec_contribution import (
    FunctionSpecMetadataContribution,
    add_function_spec_metadata,
)
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


def _create_type(system_metadata_builder=None, system_metadata_namespace=None):
    return create_function_type(
        FunctionTypeConfig(
            name="metadata_test_function",
            param_validators=[lambda type_hint: type_hint is str],
            return_validator=lambda type_hint: type_hint is str,
            param_count=1,
            system_metadata_builder=system_metadata_builder,
            system_metadata_namespace=system_metadata_namespace,
        )
    )


def _metadata_only(namespace, metadata, field="system_metadata"):
    def decorate(func):
        return add_function_spec_metadata(
            func,
            FunctionSpecMetadataContribution(
                field=field, namespace=namespace, metadata=metadata
            ),
        )

    return decorate


def _skip_export(monkeypatch):
    monkeypatch.setattr(
        MetaflowFunction,
        "_export",
        classmethod(lambda cls, func_spec, package_suffixes=None: func_spec),
    )


def test_parameterized_decorator_contributes_system_metadata(monkeypatch):
    def build_metadata(_func, *, label=None):
        return {"label": label} if label is not None else None

    Function, decorator = _create_type(build_metadata)

    @decorator(label="value")
    def handler(data: str) -> str:
        return data

    _skip_export(monkeypatch)
    function = Function(handler, task=_Task())

    namespace = f"{decorator.__module__}.{decorator.__name__}"
    assert function.spec.system_metadata[namespace] == {"label": "value"}


def test_explicit_system_metadata_namespace(monkeypatch):
    Function, decorator = _create_type(
        lambda _func, **kwargs: {"joins": ["my_join"]},
        system_metadata_namespace="feature-store",
    )

    @decorator(features=[object()])
    def handler(data: str) -> str:
        return data

    _skip_export(monkeypatch)
    function = Function(handler, task=_Task())

    assert function.spec.system_metadata["feature-store"] == {"joins": ["my_join"]}


@pytest.mark.parametrize("namespace", ["", "   ", 42])
def test_explicit_system_metadata_namespace_must_be_a_non_empty_string(namespace):
    with pytest.raises(
        TypeError, match="system_metadata_namespace must be a non-empty string"
    ):
        _create_type(
            lambda _func, **kwargs: {"label": "value"},
            system_metadata_namespace=namespace,
        )


def test_metadata_only_contributions_stack_in_either_order(monkeypatch):
    Function, decorator = _create_type(
        lambda _func, *, label: {"label": label},
        system_metadata_namespace="function-type",
    )

    @_metadata_only("outer-metadata", {"position": "outer"})
    @decorator(label="base")
    @_metadata_only("inner-metadata", {"position": "inner"})
    def handler(data: str) -> str:
        return data

    _skip_export(monkeypatch)

    function = Function(handler, task=_Task())

    assert function.spec.system_metadata["outer-metadata"] == {"position": "outer"}
    assert function.spec.system_metadata["function-type"] == {"label": "base"}
    assert function.spec.system_metadata["inner-metadata"] == {"position": "inner"}


def test_metadata_only_contribution_can_extend_user_metadata(monkeypatch):
    Function, decorator = _create_type()

    @_metadata_only(
        "decorator-metadata",
        {"owner": "payments"},
        field="user_metadata",
    )
    @decorator
    def handler(data: str) -> str:
        return data

    _skip_export(monkeypatch)
    function = Function(
        handler,
        task=_Task(),
        user_metadata={"binding-metadata": {"label": "value"}},
    )

    assert function.spec.user_metadata == {
        "binding-metadata": {"label": "value"},
        "decorator-metadata": {"owner": "payments"},
    }


def test_bare_and_empty_decorator_forms_remain_supported(monkeypatch):
    builder_calls = []

    def build_metadata(_func, *, label=None):
        builder_calls.append(label)
        return None

    Function, decorator = _create_type(build_metadata)

    @decorator
    def bare(data: str) -> str:
        return data

    @decorator()
    def empty(data: str) -> str:
        return data

    _skip_export(monkeypatch)

    namespace = f"{decorator.__module__}.{decorator.__name__}"
    assert namespace not in Function(bare, task=_Task()).spec.system_metadata
    assert namespace not in Function(empty, task=_Task()).spec.system_metadata
    assert builder_calls == [None, None]


def test_options_are_rejected_without_a_metadata_builder():
    _, decorator = _create_type()

    with pytest.raises(TypeError, match="unexpected keyword argument"):

        @decorator(label="value")
        def handler(data: str) -> str:
            return data


@pytest.mark.parametrize("metadata", [["not-a-dict"], "not-a-dict", 42])
def test_metadata_builder_must_return_a_dict_or_none(metadata):
    _, decorator = _create_type(lambda _func, **kwargs: metadata)

    with pytest.raises(TypeError, match="must return a dict or None"):

        @decorator(label="value")
        def handler(data: str) -> str:
            return data


def test_metadata_builder_output_must_be_json_serializable():
    _, decorator = _create_type(lambda _func, **kwargs: {"bad": object()})

    with pytest.raises(TypeError, match="must be JSON serializable"):

        @decorator(label="value")
        def handler(data: str) -> str:
            return data


def test_contributed_metadata_cannot_overwrite_framework_metadata(monkeypatch):
    Function, decorator = _create_type(
        lambda _func, **kwargs: {"replacement": True},
        system_metadata_namespace="environment",
    )

    @decorator(label="value")
    def handler(data: str) -> str:
        return data

    _skip_export(monkeypatch)
    with pytest.raises(MetaflowFunctionException, match="conflicts with existing"):
        Function(handler, task=_Task())
