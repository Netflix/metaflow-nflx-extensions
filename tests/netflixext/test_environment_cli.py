import os
import pytest
import shutil
import tempfile

from metaflow import FlowAPI


@pytest.fixture
def no_cached_env():
    flow_file_name = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "sample_flow.py"
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        current_dir = os.getcwd()
        os.chdir(tmpdir)
        os.mkdir(".metaflow")
        yield tmpdir, flow_file_name
        os.chdir(current_dir)


def test_show_envs(no_cached_env):
    api = FlowAPI(no_cached_env[1], environment="conda")
    res = api.environment().show()
    # Should show for all steps
    for step_name in ("start", "a", "b", "c", "end"):
        assert step_name in res
    # We should not have any other info in res
    assert len(res) == 5

    # Named environment should be resolved
    # Verify fields
    c_res = res["c"]
    assert c_res.is_disabled == False
    assert c_res.error is None
    assert c_res.requested_packages["pypi"]["pandas"] == ">=0.24.0"
    assert c_res.from_env_name == "mlp/metaflow/romain/test_pandas_env"
    assert c_res.is_fetch_at_exec == False
    assert c_res.env is not None

    # All other environments should not be resolved
    for step_name in ("start", "a"):
        rr = res[step_name]
        assert rr.is_disabled == False
        assert rr.error is None
        assert (
            rr.requested_packages["pypi" if step_name == "a" else "conda"]["pandas"]
            == ">=0.24.0"
        )
        assert rr.from_env_name is None
        assert rr.is_fetch_at_exec == False
        assert rr.env is None
    for step_name in ("b", "end"):
        rr = res[step_name]
        assert rr.is_disabled == True
        assert rr.error is None
        assert rr.requested_packages == {}
        assert rr.from_env_name is None
        assert rr.is_fetch_at_exec == False
        assert rr.env is None


def test_show_envs_single(no_cached_env):
    api = FlowAPI(no_cached_env[1], environment="conda")
    res = api.environment().show("c", timeout=600)
    assert len(res) == 1
    assert "c" in res
    # No need to check for info on "c" since this is done in the other test


def test_show_envs_timeout(no_cached_env):
    api = FlowAPI(no_cached_env[1], environment="conda")
    with pytest.raises(RuntimeError):
        api.environment().show("c", timeout=0.1)


def test_resolve_one_env(no_cached_env):
    api = FlowAPI(no_cached_env[1], environment="conda")
    res = api.environment().resolve("start", timeout=2400)
    assert len(res) == 1
    start_res = next(iter(res["start"].values()))
    assert start_res[1]  # Just resolved
    assert start_res[0]  # There is a resolved environment


def test_resolve_all_envs(no_cached_env):
    api = FlowAPI(no_cached_env[1], environment="conda")
    res = api.environment().resolve(timeout=2400)
    # Should show for all steps that are not disabled
    for step_name in ("start", "a", "c"):
        assert step_name in res
    assert len(res) == 3
    c_res = next(iter(res["c"].values()))
    assert c_res[1] == False  # Already resolved
    assert c_res[0]  # There is a resolved environment
    for step_name in ("start", "a"):
        step_res = next(iter(res[step_name].values()))
        assert step_res[1] == True  # Not previously resolved
        assert step_res[0]  # There is a resolved environment
