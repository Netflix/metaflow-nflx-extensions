import pytest

from metaflow.exception import MetaflowException
from metaflow_extensions.nflx.plugins.conda.utils import (
    resolve_env_alias,
    AliasType,
)


def test_resolve_env_alias_pathspec():
    assert resolve_env_alias("step:flow/step/task") == (
        AliasType.PATHSPEC,
        "flow/step/task",
    )
    assert resolve_env_alias("step:flow/step/task/attempt") == (
        AliasType.PATHSPEC,
        "flow/step/task",
    )


def test_resolve_env_alias_req_full_id():
    env_alias = "a" * 40 + ":" + "b" * 40
    assert resolve_env_alias(env_alias) == (AliasType.REQ_FULL_ID, env_alias)


def test_resolve_env_alias_invalid_full_id():
    env_alias = "a" * 40
    with pytest.raises(MetaflowException, match="Invalid format for environment alias"):
        resolve_env_alias(env_alias)


def test_resolve_env_alias_generic():
    assert resolve_env_alias("env_name:tag") == (AliasType.GENERIC, "env_name/tag")
    assert resolve_env_alias("env_name") == (AliasType.GENERIC, "env_name/latest")
    assert resolve_env_alias("env_name/bar/baz") == (
        AliasType.GENERIC,
        "env_name/bar/baz/latest",
    )


def test_resolve_env_alias_invalid_image_name():
    with pytest.raises(
        MetaflowException,
        match="An environment name must contain only "
        "lowercase alphanumeric characters, dashes, underscores and forward slashes.",
    ):
        resolve_env_alias("env@name:tag")


def test_resolve_env_alias_invalid_image_name_start_end():
    with pytest.raises(
        MetaflowException,
        match="An environment name must not start or end with '/'",
    ):
        resolve_env_alias("/env_name:tag")
    with pytest.raises(
        MetaflowException,
        match="An environment name must not start or end with '/'",
    ):
        resolve_env_alias("env_name/:tag")


def test_resolve_env_alias_invalid_tag_name():
    with pytest.raises(
        MetaflowException,
        match="An environment tag name must contain only "
        "lowercase alphanumeric characters, dashes and underscores.",
    ):
        resolve_env_alias("env_name:tag@name")
