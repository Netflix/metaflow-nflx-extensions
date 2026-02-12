import os

os.environ["FUNCTION_CLEAN_DIR_ON_DEL"] = "1"

import pytest

pytestmark = pytest.mark.no_backend_parametrization
from typing import Any, Dict
import pandas as pd
from metaflow import FunctionParameters


class MockArtifact:
    def __init__(self, data):
        self.data = data


class MockTask:
    def __init__(self, artifacts):
        self.artifacts = {name: MockArtifact(data) for name, data in artifacts.items()}

    def __getitem__(self, name):
        try:
            return self.artifacts[name]
        except KeyError:
            raise KeyError(f"Artifact '{name}' not found in FunctionParameters.")


def test_function_params_with_task_only():
    task = MockTask({"x": 1, "y": 2})
    fd = FunctionParameters(task=task)
    assert fd["x"] == 1
    assert fd["y"] == 2
    assert fd.x == 1
    assert fd.y == 2
    with pytest.raises(KeyError, match="Artifact 'z' not found in FunctionParameters."):
        _ = fd["z"]
    with pytest.raises(
        AttributeError, match="'FunctionParameters' object has no attribute 'z'"
    ):
        _ = fd.z


def test_function_params_with_overrides_only():
    fd = FunctionParameters(a=10, b=20)
    assert fd["a"] == 10
    assert fd["b"] == 20
    assert fd.a == 10
    assert fd.b == 20
    with pytest.raises(KeyError, match="Artifact 'c' not found in FunctionParameters."):
        _ = fd["c"]
    with pytest.raises(
        AttributeError, match="'FunctionParameters' object has no attribute 'c'"
    ):
        _ = fd.c


def test_function_params_with_task_and_overrides():
    task = MockTask({"x": 1, "y": 2})
    fd = FunctionParameters(task=task, x=10, z=30)
    assert fd["x"] == 10
    assert fd["y"] == 2
    assert fd["z"] == 30
    assert fd.x == 10
    assert fd.y == 2
    assert fd.z == 30
    with pytest.raises(KeyError, match="Artifact 'w' not found in FunctionParameters."):
        _ = fd["w"]
    with pytest.raises(
        AttributeError, match="'FunctionParameters' object has no attribute 'w'"
    ):
        _ = fd.w


def test_function_params_no_task_no_overrides():
    fd = FunctionParameters()
    with pytest.raises(KeyError, match="Artifact 'x' not found in FunctionParameters."):
        _ = fd["x"]
    with pytest.raises(
        AttributeError, match="'FunctionParameters' object has no attribute 'x'"
    ):
        _ = fd.x


def test_cannot_set_attributes_after_init():
    fd = FunctionParameters(a=1)
    with pytest.raises(
        AttributeError, match="Cannot modify FunctionParameters after initialization."
    ):
        fd.a = 2
    with pytest.raises(
        TypeError, match="'FunctionParameters' object does not support item assignment"
    ):
        fd["a"] = 2


# Tests for typed artifact specification
class TypedModelParams(FunctionParameters):
    model: Any
    features: Dict[str, Any]
    config: Dict[str, str]


def test_typed_function_params_basic():
    """Test basic typed function parameters functionality."""
    task = MockTask(
        {"model": "my_model", "features": {"feat1": 1}, "config": {"param": "value"}}
    )
    params = TypedModelParams(task=task)

    assert params.model == "my_model"
    assert params.features == {"feat1": 1}
    assert params.config == {"param": "value"}
    assert params["model"] == "my_model"
    assert params["features"] == {"feat1": 1}


def test_typed_function_params_missing_artifact():
    """Test that typed function parameters raise error for missing artifacts."""
    task = MockTask({"model": "my_model", "features": {"feat1": 1}})  # Missing config

    with pytest.raises(
        KeyError,
        match="Task is missing required artifacts for TypedModelParams: \\['config'\\]",
    ):
        TypedModelParams(task=task)


def test_typed_function_params_with_overrides():
    """Test typed function parameters with override values."""
    task = MockTask({"model": "my_model", "features": {"feat1": 1}})  # Missing config

    # Should work because we provide config as override
    params = TypedModelParams(task=task, config={"override": "value"})
    assert params.model == "my_model"
    assert params.features == {"feat1": 1}
    assert params.config == {"override": "value"}


def test_typed_function_params_extra_artifact_access():
    """Test that typed function parameters prevent access to non-specified artifacts."""
    task = MockTask(
        {
            "model": "my_model",
            "features": {"feat1": 1},
            "config": {"param": "value"},
            "extra": "data",
        }
    )
    params = TypedModelParams(task=task)

    # Can access specified artifacts
    assert params.model == "my_model"

    # Cannot access non-specified artifacts
    with pytest.raises(
        KeyError,
        match="Artifact 'extra' is not specified in the typed function parameters class",
    ):
        _ = params["extra"]

    with pytest.raises(AttributeError):
        _ = params.extra


def test_typed_function_params_iteration():
    """Test iteration over typed function parameters."""
    task = MockTask(
        {"model": "my_model", "features": {"feat1": 1}, "config": {"param": "value"}}
    )
    params = TypedModelParams(task=task)

    # Should iterate only over expected artifacts
    artifacts = set(params)
    expected = {"model", "features", "config"}
    assert artifacts == expected


def test_get_expected_artifacts():
    """Test get_expected_artifacts class method."""
    expected = TypedModelParams.get_expected_artifacts()
    assert expected == {"model", "features", "config"}

    # Regular FunctionParameters should return None
    assert FunctionParameters.get_expected_artifacts() is None


def test_validate_task_compatibility():
    """Test validate_task_compatibility class method."""
    # Compatible task
    good_task = MockTask(
        {"model": "my_model", "features": {"feat1": 1}, "config": {"param": "value"}}
    )
    assert TypedModelParams.validate_task_compatibility(good_task) is True

    # Incompatible task (missing artifact)
    bad_task = MockTask(
        {"model": "my_model", "features": {"feat1": 1}}
    )  # Missing config
    assert TypedModelParams.validate_task_compatibility(bad_task) is False


def test_backwards_compatibility():
    """Test that regular FunctionParameters still works as before."""
    task = MockTask({"x": 1, "y": 2, "z": 3})
    params = FunctionParameters(task=task)

    # Should be able to access any artifact
    assert params.x == 1
    assert params.y == 2
    assert params.z == 3

    # Should iterate over all artifacts
    artifacts = set(params)
    assert "x" in artifacts
    assert "y" in artifacts
    assert "z" in artifacts


def test_inheritance_chain():
    """Test that inheritance chain works correctly."""

    class SubTypedParams(TypedModelParams):
        extra_field: str

    # Should inherit parent's expected artifacts plus new ones
    expected = SubTypedParams.get_expected_artifacts()
    assert "model" in expected
    assert "features" in expected
    assert "config" in expected
    assert "extra_field" in expected


# Tests for artifact pre-fetching
class MockFunctionSpec:
    """Mock FunctionSpec for testing artifact pre-fetching."""

    def __init__(self, artifacts_dict):
        self.artifacts = artifacts_dict
        self.task_pathspec = "TestFlow/123/test_step/456"


class MockDataArtifact:
    """Mock DataArtifact that simulates S3 downloads."""

    def __init__(self, value, should_fail=False):
        self._value = value
        self._should_fail = should_fail
        self._accessed = False

    @property
    def data(self):
        """Simulate downloading from S3."""
        self._accessed = True
        if self._should_fail:
            raise RuntimeError("Simulated S3 download failure")
        return self._value


def test_prefetch_artifacts_disabled_by_default():
    """Test that artifacts are not pre-fetched by default (lazy loading)."""
    from unittest.mock import Mock, patch, call

    artifact_metadata = {"model": {"type": "str"}}
    func_spec = MockFunctionSpec(artifact_metadata)

    import metaflow_extensions.nflx.plugins.functions.core.function_parameters as fp_module

    mock_mapping = Mock()
    mock_mapping.__getitem__ = Mock(return_value="data_for_model")
    mock_mapping.__iter__ = Mock(return_value=iter(["model"]))
    mock_mapping.__len__ = Mock(return_value=1)

    with patch.object(fp_module, "LazyArtifactMapping", return_value=mock_mapping):
        # Create FunctionParameters WITHOUT pre-fetch
        params = FunctionParameters(function_spec=func_spec, prefetch_artifacts=False)

        # Artifacts should NOT have been accessed during init (lazy loading)
        assert mock_mapping.__getitem__.call_count == 0

        # Now access an artifact - should work via lazy loading
        result = params["model"]
        assert result == "data_for_model"
        assert mock_mapping.__getitem__.call_args_list == [call("model")]


def test_prefetch_artifacts_enabled():
    """Test that prefetch_artifacts=True downloads all artifacts during init."""
    from unittest.mock import Mock, patch, call

    artifact_metadata = {
        "model": {"type": "str"},
        "config": {"type": "dict"},
        "features": {"type": "list"},
    }
    func_spec = MockFunctionSpec(artifact_metadata)

    import metaflow_extensions.nflx.plugins.functions.core.function_parameters as fp_module

    mock_mapping = Mock()
    mock_mapping.__getitem__ = Mock(side_effect=lambda k: f"data_{k}")
    mock_mapping.__iter__ = Mock(return_value=iter(["model", "config", "features"]))
    mock_mapping.__len__ = Mock(return_value=3)

    with patch.object(fp_module, "LazyArtifactMapping", return_value=mock_mapping):
        # Create FunctionParameters WITH pre-fetch
        params = FunctionParameters(function_spec=func_spec, prefetch_artifacts=True)

        # All artifacts should have been accessed during init
        assert mock_mapping.__getitem__.call_count == 3
        accessed = {
            call_args[0][0] for call_args in mock_mapping.__getitem__.call_args_list
        }
        assert accessed == {"model", "config", "features"}


def test_prefetch_artifacts_fails_fast_on_error():
    """Test that pre-fetch failures raise exceptions immediately."""
    from unittest.mock import Mock, patch

    artifact_metadata = {
        "model": {"type": "str"},
        "bad_artifact": {"type": "str"},
        "config": {"type": "dict"},
    }
    func_spec = MockFunctionSpec(artifact_metadata)

    import metaflow_extensions.nflx.plugins.functions.core.function_parameters as fp_module

    def failing_getitem(key):
        if key == "bad_artifact":
            raise RuntimeError(f"Failed to download artifact '{key}' from S3")
        return f"data_{key}"

    mock_mapping = Mock()
    mock_mapping.__getitem__ = Mock(side_effect=failing_getitem)
    mock_mapping.__iter__ = Mock(return_value=iter(["model", "bad_artifact", "config"]))
    mock_mapping.__len__ = Mock(return_value=3)

    with patch.object(fp_module, "LazyArtifactMapping", return_value=mock_mapping):
        # Pre-fetch should fail fast and raise exception
        with pytest.raises(RuntimeError, match="Failed to download artifact"):
            FunctionParameters(function_spec=func_spec, prefetch_artifacts=True)


def test_prefetch_artifacts_with_empty_artifacts():
    """Test that pre-fetch handles empty artifact list gracefully."""
    # Empty artifacts
    func_spec = MockFunctionSpec({})

    # Should not raise exception
    params = FunctionParameters(function_spec=func_spec, prefetch_artifacts=True)

    # Should still work as normal FunctionParameters
    with pytest.raises(KeyError):
        _ = params["nonexistent"]


def test_prefetch_artifacts_with_none_function_spec():
    """Test that pre-fetch handles None function_spec gracefully."""
    # No function_spec provided
    params = FunctionParameters(prefetch_artifacts=True, model="test_data")

    # Should work with overrides
    assert params["model"] == "test_data"

    # Should not have any other artifacts
    with pytest.raises(KeyError):
        _ = params["nonexistent"]


def test_prefetch_only_affects_function_spec_artifacts():
    """Test that pre-fetch only affects artifacts from function_spec, not overrides."""
    from unittest.mock import Mock, patch, call

    artifact_metadata = {"model": {"type": "str"}}
    func_spec = MockFunctionSpec(artifact_metadata)

    import metaflow_extensions.nflx.plugins.functions.core.function_parameters as fp_module

    mock_mapping = Mock()
    mock_mapping.__getitem__ = Mock(side_effect=lambda k: f"data_{k}")
    mock_mapping.__iter__ = Mock(return_value=iter(["model"]))
    mock_mapping.__len__ = Mock(return_value=1)

    with patch.object(fp_module, "LazyArtifactMapping", return_value=mock_mapping):
        # Create with override AND pre-fetch
        params = FunctionParameters(
            function_spec=func_spec,
            prefetch_artifacts=True,
            override_val="override_data",
        )

        # Only 'model' should be accessed during prefetch (override_val is in cache already)
        accessed = [
            call_args[0][0] for call_args in mock_mapping.__getitem__.call_args_list
        ]
        assert "model" in accessed
        assert "override_val" not in accessed
