"""Resolution of a function's *package directory* -- the directory inside an
extracted code package that holds the function's module, and therefore the
files a model owner colocated with their code (an .avsc schema, a .json
config, ...).

This is deliberately not the same thing as the extraction root.
MetaflowFunctionPackage archives a function's files relative to the parent of
its top-level package, so a function in ``a.b.c`` is stored at ``a/b/c.py``
and its colocated files land in ``<extraction root>/a/b``. Only a top-level
module puts them at the extraction root itself.
"""

import os

import pytest

from metaflow_extensions.nflx.plugins.functions.config import Config
from metaflow_extensions.nflx.plugins.functions.core.function_pipeline_spec import (
    FunctionPipelineSpec,
)
from metaflow_extensions.nflx.plugins.functions.core.function_spec import FunctionSpec
from metaflow_extensions.nflx.plugins.functions.utils import (
    module_package_subpath,
    resolve_package_dir,
)


def _dir_for(base_path, uuid):
    return os.path.join(base_path, f"{Config.RUNTIME_FUNCTION_DIR_PREFIX}{uuid}")


class _PlainSpec(FunctionSpec):
    """Minimal concrete FunctionSpec -- FunctionSpec is an ABC."""

    @classmethod
    def _from_json_impl_from_data(cls, desc):
        raise NotImplementedError


class _Deco:
    def __init__(self, module):
        self.module = module


@pytest.mark.parametrize(
    "module,expected",
    [
        ("a.b.c", os.path.join("a", "b")),
        ("a.b", "a"),
        ("c", ""),
        ("", ""),
        (None, ""),
    ],
)
def test_module_package_subpath(module, expected):
    assert module_package_subpath(module) == expected


def test_resolve_package_dir_descends_to_module_dir(tmp_path):
    root = str(tmp_path)
    os.makedirs(os.path.join(root, "a", "b"))
    assert resolve_package_dir(root, "a.b.c") == os.path.join(root, "a", "b")


def test_resolve_package_dir_top_level_module_is_root(tmp_path):
    root = str(tmp_path)
    assert resolve_package_dir(root, "c") == root


def test_resolve_package_dir_falls_back_when_subdir_missing(tmp_path):
    """A package built with different arcnames (or a partially extracted dir)
    must not resolve to a path that doesn't exist -- fall back to the root,
    which is exactly the behavior that predates package-dir resolution."""
    root = str(tmp_path)
    assert resolve_package_dir(root, "not.here.at.all") == root


def test_spec_package_dir_is_nested_under_extraction_root(tmp_path):
    base_path = str(tmp_path)
    root = _dir_for(base_path, "fn-uuid")
    os.makedirs(os.path.join(root, "pkg", "sub"))

    spec = _PlainSpec(uuid="fn-uuid", function=_Deco("pkg.sub.mod"))

    assert spec.resolve_function_root_dir(base_path) == root
    assert spec.resolve_function_package_dir(base_path) == os.path.join(
        root, "pkg", "sub"
    )


def test_spec_package_dir_equals_root_for_flat_layout(tmp_path):
    base_path = str(tmp_path)
    root = _dir_for(base_path, "fn-uuid")
    os.makedirs(root)

    spec = _PlainSpec(uuid="fn-uuid", function=_Deco("mod"))

    assert spec.resolve_function_package_dir(base_path) == root


def test_pipeline_spec_delegates_to_first_constituent(tmp_path):
    """The pipeline's own uuid *and* its own module are both wrong for
    locating colocated files: its directory is near-empty, and its `function`
    points at FunctionPipeline's own module."""
    base_path = str(tmp_path)
    child_root = _dir_for(base_path, "child-uuid")
    os.makedirs(os.path.join(child_root, "pkg", "sub"))

    spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="fake.module.FakePipeline",
        function=_Deco(
            "metaflow_extensions.nflx.plugins.functions.core.function_pipeline"
        ),
        system_metadata={
            "functions": [
                {
                    "uuid": "child-uuid",
                    "class_name": "fake.module.FakeFunction",
                    "function": {"module": "pkg.sub.mod"},
                },
                {
                    "uuid": "other-uuid",
                    "class_name": "fake.module.FakeFunction",
                    "function": {"module": "other.mod"},
                },
            ]
        },
    )

    assert spec.resolve_function_root_dir(base_path) == child_root
    assert spec.resolve_function_package_dir(base_path) == os.path.join(
        child_root, "pkg", "sub"
    )


def test_pipeline_spec_descends_through_nested_pipeline(tmp_path):
    base_path = str(tmp_path)
    leaf_root = _dir_for(base_path, "leaf-uuid")
    os.makedirs(os.path.join(leaf_root, "pkg"))

    inner = {
        "uuid": "inner-uuid",
        "class_name": (
            "metaflow_extensions.nflx.plugins.functions.core."
            "function_pipeline.FunctionPipeline"
        ),
        "system_metadata": {
            "functions": [
                {
                    "uuid": "leaf-uuid",
                    "class_name": "fake.module.FakeFunction",
                    "function": {"module": "pkg.mod"},
                }
            ]
        },
    }
    spec = FunctionPipelineSpec(
        uuid="outer-uuid",
        class_name="fake.module.FakePipeline",
        system_metadata={"functions": [inner]},
    )

    assert spec.resolve_function_package_dir(base_path) == os.path.join(
        leaf_root, "pkg"
    )


def test_pipeline_spec_falls_back_to_own_dir_without_constituents(tmp_path):
    """A pipeline spec carrying no constituent specs has nothing to delegate
    to; it must still return its own directory rather than raise."""
    base_path = str(tmp_path)
    spec = FunctionPipelineSpec(uuid="pipeline-uuid", class_name="x.FakePipeline")

    expected = _dir_for(base_path, "pipeline-uuid")
    assert spec.resolve_function_root_dir(base_path) == expected
    assert spec.resolve_function_package_dir(base_path) == expected


def test_pipeline_spec_resolution_imports_nothing(tmp_path, monkeypatch):
    """Caller-side resolution runs on the proxy path, which deliberately
    avoids importing function code -- resolving a directory must not drag a
    constituent's class into the process."""
    import metaflow_extensions.nflx.plugins.functions.core.function_spec as fs_mod

    def _boom(*args, **kwargs):
        raise AssertionError("resolution must not import constituent classes")

    monkeypatch.setattr(fs_mod, "load_class_from_string", _boom)

    base_path = str(tmp_path)
    os.makedirs(os.path.join(_dir_for(base_path, "child-uuid"), "pkg"))
    spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="fake.module.FakePipeline",
        system_metadata={
            "functions": [
                {
                    "uuid": "child-uuid",
                    "class_name": "definitely.not.importable.Thing",
                    "function": {"module": "pkg.mod"},
                }
            ]
        },
    )

    assert spec.resolve_function_package_dir(base_path) == os.path.join(
        _dir_for(base_path, "child-uuid"), "pkg"
    )


# ---------------------------------------------------------------------------
# Runtime side: MetaflowFunction.function_package_dir
#
# The caller-side hook resolves a directory from a spec; the runtime side
# resolves it from the reconstructed instance. Both have to agree, or a
# component's start() reads a different directory than its
# on_runtime_started() did.
# ---------------------------------------------------------------------------


def _bare_function(function_root_dir, module):
    """A reconstructed function, built the way from_spec() builds one
    (__new__, then attributes) -- MetaflowFunction is an ABC, so this needs a
    concrete subclass."""
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        MetaflowFunction,
    )

    class _ConcreteFunction(MetaflowFunction):
        @property
        def input_types(self):
            return {}

        @property
        def output_types(self):
            return {}

        def is_compatible_with(self, other):
            return True

    fn = _ConcreteFunction.__new__(_ConcreteFunction)
    fn._function_root_dir = function_root_dir
    fn._function_spec = _PlainSpec(uuid="fn-uuid", function=_Deco(module))
    return fn


def test_function_package_dir_matches_spec_resolution(tmp_path):
    base_path = str(tmp_path)
    root = _dir_for(base_path, "fn-uuid")
    os.makedirs(os.path.join(root, "pkg", "sub"))

    spec = _PlainSpec(uuid="fn-uuid", function=_Deco("pkg.sub.mod"))
    fn = _bare_function(root, "pkg.sub.mod")

    assert fn.function_package_dir == spec.resolve_function_package_dir(base_path)
    assert fn.function_package_dir == os.path.join(root, "pkg", "sub")


def test_function_package_dir_is_root_for_flat_layout(tmp_path):
    root = str(tmp_path)
    assert _bare_function(root, "mod").function_package_dir == root


def test_function_package_dir_raises_when_root_unset():
    from metaflow_extensions.nflx.plugins.functions.exceptions import (
        MetaflowFunctionException,
    )

    fn = _bare_function(None, "pkg.mod")
    with pytest.raises(MetaflowFunctionException):
        _ = fn.function_package_dir


def test_pipeline_function_package_dir_delegates_to_first_constituent(tmp_path):
    """FunctionPipeline's own spec.function points at FunctionPipeline's
    module, so the base implementation would resolve somewhere unrelated to
    the model owner's code."""
    from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
        FunctionPipeline,
    )

    base_path = str(tmp_path)
    child_root = _dir_for(base_path, "child-uuid")
    os.makedirs(os.path.join(child_root, "pkg", "sub"))

    child = _bare_function(child_root, "pkg.sub.mod")

    pipeline = FunctionPipeline.__new__(FunctionPipeline)
    pipeline._function_root_dir = child_root
    pipeline._function_spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="x.FakePipeline",
        function=_Deco(
            "metaflow_extensions.nflx.plugins.functions.core.function_pipeline"
        ),
    )
    pipeline.functions = [child]

    assert pipeline.function_package_dir == os.path.join(child_root, "pkg", "sub")


# ---------------------------------------------------------------------------
# ensure_function_package_extracted
#
# Resolving the right path isn't enough: a caller-side component reads files
# out of a directory that, for a pipeline, is normally only ever extracted
# inside the runtime subprocess.
# ---------------------------------------------------------------------------


def _patch_extract(monkeypatch):
    """Record extract_code_packages() calls instead of hitting S3."""
    import metaflow_extensions.nflx.plugins.functions.environment as env_mod

    calls = []

    def _fake(code_package, task_code_path, base_path):
        calls.append((code_package, task_code_path, base_path))
        return base_path

    monkeypatch.setattr(env_mod, "extract_code_packages", _fake)
    return calls


def test_ensure_extracted_uses_own_package(tmp_path, monkeypatch):
    calls = _patch_extract(monkeypatch)
    base_path = str(tmp_path)

    spec = _PlainSpec(
        uuid="fn-uuid",
        function=_Deco("pkg.mod"),
        code_package="s3://bucket/fn.zip",
        task_code_path="s3://bucket/task.tgz",
    )
    spec.ensure_function_package_extracted(base_path)

    assert calls == [
        ("s3://bucket/fn.zip", "s3://bucket/task.tgz", _dir_for(base_path, "fn-uuid"))
    ]


def test_ensure_extracted_pipeline_uses_first_constituents_package(
    tmp_path, monkeypatch
):
    """The pipeline's own package is a near-empty placeholder -- extracting it
    would leave the caller with a directory that has none of the model
    owner's files in it."""
    calls = _patch_extract(monkeypatch)
    base_path = str(tmp_path)

    spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="x.FakePipeline",
        code_package="s3://bucket/pipeline-placeholder.zip",
        task_code_path="s3://bucket/task.tgz",
        system_metadata={
            "functions": [
                {
                    "uuid": "child-uuid",
                    "class_name": "fake.module.FakeFunction",
                    "function": {"module": "pkg.sub.mod"},
                    "code_package": "s3://bucket/child.zip",
                    "task_code_path": "s3://bucket/task.tgz",
                }
            ]
        },
    )
    spec.ensure_function_package_extracted(base_path)

    assert calls == [
        (
            "s3://bucket/child.zip",
            "s3://bucket/task.tgz",
            _dir_for(base_path, "child-uuid"),
        )
    ]


def test_ensure_extracted_noop_without_package_paths(tmp_path, monkeypatch):
    """Nothing to fetch -- must not raise, and must not call into S3."""
    calls = _patch_extract(monkeypatch)
    spec = _PlainSpec(uuid="fn-uuid", function=_Deco("pkg.mod"))
    spec.ensure_function_package_extracted(str(tmp_path))
    assert calls == []


def test_function_from_json_pre_extracts_for_caller_side_components(
    tmp_path, monkeypatch
):
    """End-to-end through function_from_json: the package is extracted before
    on_runtime_started fires, so the component can actually read from the
    directory it's handed."""
    from unittest.mock import patch, MagicMock

    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    import metaflow_extensions.nflx.plugins.functions.environment as env_mod

    base_path = str(tmp_path)
    child_root = _dir_for(base_path, "child-uuid")

    def _fake_extract(code_package, task_code_path, dest):
        # Stand in for the real download+unzip: lay the package down the way
        # MetaflowFunctionPackage archives it, under the module's path.
        os.makedirs(os.path.join(dest, "pkg", "sub"), exist_ok=True)
        with open(os.path.join(dest, "pkg", "sub", "schema.avsc"), "w") as f:
            f.write('{"type": "record"}')
        return dest

    monkeypatch.setattr(env_mod, "extract_code_packages", _fake_extract)

    fake_spec = FunctionPipelineSpec(
        uuid="pipeline-uuid",
        class_name="fake.module.FakePipeline",
        system_metadata={
            "functions": [
                {
                    "uuid": "child-uuid",
                    "class_name": "fake.module.FakeFunction",
                    "function": {"module": "pkg.sub.mod"},
                    "code_package": "s3://bucket/child.zip",
                    "task_code_path": "s3://bucket/task.tgz",
                }
            ]
        },
    )

    fake_func = MagicMock()
    fake_func._runtime_components = []
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    class _SchemaReader(AbstractRuntimeComponent):
        component_id = "clu_schema_reader"

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.contents = None

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

        def on_runtime_started(self, function_package_dir):
            with open(os.path.join(function_package_dir, "schema.avsc")) as f:
                self.contents = f.read()

    component = _SchemaReader()

    assert not os.path.isdir(child_root)

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        function_from_json(
            "fake-reference.json",
            base_path=base_path,
            start_runtime=True,
            runtime_components=[component],
        )

    assert component.contents == '{"type": "record"}'


def test_function_from_json_survives_failed_pre_extraction(tmp_path, monkeypatch):
    """A component that reads no files must not be broken by a failure to
    fetch a package it never needed."""
    from unittest.mock import patch, MagicMock

    from metaflow_extensions.nflx.plugins.functions.components.abstract_component import (
        AbstractRuntimeComponent,
    )
    from metaflow_extensions.nflx.plugins.functions.core.function import (
        function_from_json,
    )
    import metaflow_extensions.nflx.plugins.functions.environment as env_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("S3 is having a day")

    monkeypatch.setattr(env_mod, "extract_code_packages", _boom)

    fake_spec = _PlainSpec(
        uuid="fn-uuid",
        class_name="fake.module.FakeFunction",
        function=_Deco("pkg.mod"),
        code_package="s3://bucket/fn.zip",
        task_code_path="s3://bucket/task.tgz",
    )
    fake_spec.serializer_configs = None

    fake_func = MagicMock()
    fake_func._runtime_components = []
    fake_subclass = MagicMock()
    fake_subclass._create_proxy_from_spec.return_value = fake_func

    class _Indifferent(AbstractRuntimeComponent):
        component_id = "clu_indifferent"

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.called = False

        def start(self, *args, **kwargs):
            pass

        def stop(self, *args, **kwargs):
            pass

        def before_call(self, *args, **kwargs):
            pass

        def after_call(self, *args, **kwargs):
            pass

        def on_runtime_started(self, function_package_dir):
            self.called = True

    component = _Indifferent()

    with patch(
        "metaflow_extensions.nflx.plugins.functions.core.function_spec.FunctionSpec.from_json",
        return_value=fake_spec,
    ), patch(
        "metaflow_extensions.nflx.plugins.functions.utils.load_type_from_string",
        return_value=fake_subclass,
    ):
        function_from_json(
            "fake-reference.json",
            base_path=str(tmp_path),
            start_runtime=True,
            runtime_components=[component],
        )

    assert component.called
