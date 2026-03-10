"""
Unit tests for resolver classes and helper functions.

These tests focus on testing resolver infrastructure, helper methods,
and error conditions without requiring full environment resolution.
"""

import pytest
from unittest.mock import Mock

# Import metaflow first to avoid circular import issues
import metaflow

from metaflow_extensions.netflix_ext.plugins.conda.envsresolver import EnvsResolver
from metaflow_extensions.netflix_ext.plugins.conda.resolvers import Resolver
from metaflow_extensions.netflix_ext.plugins.conda.resolvers.conda_resolver import (
    CondaResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.resolvers.pip_resolver import (
    PipResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.resolvers.conda_lock_resolver import (
    CondaLockResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.resolvers.pylock_toml_resolver import (
    PylockTomlResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.resolvers.builder_envs_resolver import (
    BuilderEnvsResolver,
)
from metaflow_extensions.netflix_ext.plugins.conda.utils import CondaException
from metaflow_extensions.netflix_ext.plugins.conda.env_descr import EnvType


# ===== Resolver Base Class Tests =====


def test_resolver_class_registration():
    """Test that resolver subclasses are properly registered."""
    # Ensure the class registry is initialized
    Resolver._ensure_class_per_type()

    assert Resolver._class_per_type is not None

    # Check that all known resolver types are registered
    assert "conda" in Resolver._class_per_type
    assert "pip" in Resolver._class_per_type
    assert "mamba" in Resolver._class_per_type
    assert "micromamba" in Resolver._class_per_type
    assert "conda-lock" in Resolver._class_per_type
    assert "pylock_toml" in Resolver._class_per_type
    assert "builderenv" in Resolver._class_per_type


def test_resolver_get_conda_resolver():
    """Test getting conda resolver."""
    resolver_class = Resolver.get_resolver("conda")
    assert resolver_class == CondaResolver


def test_resolver_get_mamba_resolver():
    """Test getting mamba resolver (should be CondaResolver)."""
    resolver_class = Resolver.get_resolver("mamba")
    assert resolver_class == CondaResolver


def test_resolver_get_pip_resolver():
    """Test getting pip resolver."""
    resolver_class = Resolver.get_resolver("pip")
    assert resolver_class == PipResolver


def test_resolver_get_uv_resolver():
    """Test getting uv resolver (should be aliased to pip)."""
    resolver_class = Resolver.get_resolver("uv")
    assert resolver_class == PipResolver


def test_resolver_get_conda_lock_resolver():
    """Test getting conda-lock resolver."""
    resolver_class = Resolver.get_resolver("conda-lock")
    assert resolver_class == CondaLockResolver


def test_resolver_get_pylock_toml_resolver():
    """Test getting pylock-toml resolver."""
    resolver_class = Resolver.get_resolver("pylock_toml")
    assert resolver_class == PylockTomlResolver


def test_resolver_get_builder_resolver():
    """Test getting builder resolver."""
    resolver_class = Resolver.get_resolver("builderenv")
    assert resolver_class == BuilderEnvsResolver


def test_resolver_get_invalid_resolver():
    """Test that getting an invalid resolver raises an exception."""
    with pytest.raises(CondaException) as exc_info:
        Resolver.get_resolver("invalid_resolver_type")
    assert "does not exist" in str(exc_info.value)


def test_resolver_init():
    """Test that resolver can be initialized with a conda object."""
    mock_conda = Mock()
    resolver = Resolver(mock_conda)
    assert resolver._conda == mock_conda


def test_resolver_resolve_not_implemented():
    """Test that base resolver resolve() raises NotImplementedError."""
    mock_conda = Mock()
    resolver = Resolver(mock_conda)

    with pytest.raises(NotImplementedError):
        resolver.resolve(
            env_type=EnvType.CONDA_ONLY,
            python_version_requested="3.10",
            deps={"conda": ["pandas"]},
            sources={},
            extras={},
            architecture="linux-64",
        )


def test_resolver_types():
    """Test that resolver types are properly defined."""
    assert Resolver.TYPES == ["invalid"]
    assert "conda" in CondaResolver.TYPES
    assert "mamba" in CondaResolver.TYPES
    assert "micromamba" in CondaResolver.TYPES
    assert "pip" in PipResolver.TYPES
    assert "conda-lock" in CondaLockResolver.TYPES
    assert "pylock_toml" in PylockTomlResolver.TYPES
    assert "builderenv" in BuilderEnvsResolver.TYPES


def test_resolver_requires_builder_env_flag():
    """Test that REQUIRES_BUILDER_ENV flag is properly set for resolvers."""
    # Base Resolver should not require builder env
    assert Resolver.REQUIRES_BUILDER_ENV == False

    # PipResolver should require builder env
    assert PipResolver.REQUIRES_BUILDER_ENV == True

    # CondaResolver should not require builder env
    assert CondaResolver.REQUIRES_BUILDER_ENV == False


# ===== Conda Resolver Specific Tests =====


def test_conda_resolver_init():
    """Test CondaResolver initialization."""
    mock_conda = Mock()
    resolver = CondaResolver(mock_conda)
    assert resolver._conda == mock_conda
    assert isinstance(resolver, Resolver)


def test_conda_resolver_types():
    """Test that CondaResolver supports expected types."""
    assert "conda" in CondaResolver.TYPES
    assert "mamba" in CondaResolver.TYPES
    assert "micromamba" in CondaResolver.TYPES


# ===== Pip Resolver Specific Tests =====


def test_pip_resolver_init():
    """Test PipResolver initialization."""
    mock_conda = Mock()
    resolver = PipResolver(mock_conda)
    assert resolver._conda == mock_conda
    assert isinstance(resolver, Resolver)


def test_pip_resolver_types():
    """Test that PipResolver supports expected types."""
    assert "pip" in PipResolver.TYPES


def test_pip_resolver_requires_builder():
    """Test that PipResolver requires a builder environment."""
    assert PipResolver.REQUIRES_BUILDER_ENV == True


# ===== CondaLock Resolver Specific Tests =====


def test_conda_lock_resolver_init():
    """Test CondaLockResolver initialization."""
    mock_conda = Mock()
    resolver = CondaLockResolver(mock_conda)
    assert resolver._conda == mock_conda
    assert isinstance(resolver, Resolver)


def test_conda_lock_resolver_types():
    """Test that CondaLockResolver supports expected types."""
    assert "conda-lock" in CondaLockResolver.TYPES


# ===== PylockToml Resolver Specific Tests =====


def test_pylock_toml_resolver_init():
    """Test PylockTomlResolver initialization."""
    mock_conda = Mock()
    resolver = PylockTomlResolver(mock_conda)
    assert resolver._conda == mock_conda
    assert isinstance(resolver, Resolver)


def test_pylock_toml_resolver_types():
    """Test that PylockTomlResolver supports expected types."""
    assert "pylock_toml" in PylockTomlResolver.TYPES


# ===== BuilderEnvs Resolver Specific Tests =====


def test_builder_envs_resolver_init():
    """Test BuilderEnvsResolver initialization."""
    mock_conda = Mock()
    resolver = BuilderEnvsResolver(mock_conda)
    assert resolver._conda == mock_conda
    assert isinstance(resolver, Resolver)


def test_builder_envs_resolver_types():
    """Test that BuilderEnvsResolver supports expected types."""
    assert "builderenv" in BuilderEnvsResolver.TYPES


# ===== Resolver Registry Tests =====


def test_resolver_registry_is_shared():
    """Test that the resolver registry is shared across calls."""
    Resolver._ensure_class_per_type()
    registry1 = Resolver._class_per_type

    Resolver._ensure_class_per_type()
    registry2 = Resolver._class_per_type

    # Should be the same object
    assert registry1 is registry2


def test_resolver_registry_contains_all_resolvers():
    """Test that all resolver subclasses are in the registry."""
    Resolver._ensure_class_per_type()

    # Get all registered types
    all_types = set()
    for resolver_class in [
        CondaResolver,
        PipResolver,
        CondaLockResolver,
        PylockTomlResolver,
        BuilderEnvsResolver,
    ]:
        all_types.update(resolver_class.TYPES)

    # Check that all types are in the registry
    for resolver_type in all_types:
        assert resolver_type in Resolver._class_per_type


def test_multiple_resolver_get_calls():
    """Test that multiple get_resolver calls work correctly."""
    # Call get_resolver multiple times
    resolver1 = Resolver.get_resolver("conda")
    resolver2 = Resolver.get_resolver("pip")
    resolver3 = Resolver.get_resolver("conda")

    assert resolver1 == CondaResolver
    assert resolver2 == PipResolver
    assert resolver3 == CondaResolver


def test_resolver_get_all_variants():
    """Test getting all conda resolver variants."""
    conda = Resolver.get_resolver("conda")
    mamba = Resolver.get_resolver("mamba")
    micromamba = Resolver.get_resolver("micromamba")

    # All should point to CondaResolver
    assert conda == mamba == micromamba == CondaResolver


# ===== Error Handling Tests =====


def test_resolver_invalid_type_string():
    """Test various invalid resolver type strings."""
    invalid_types = [
        "invalid",
        "unknown",
        "conda_lock",  # Note: should be "conda-lock"
        "pylock-toml",  # Note: uses underscore not dash
        "builder",  # Note: should be "builderenv"
        "CONDA",  # Case sensitive
        "PIP",  # Case sensitive
    ]

    for invalid_type in invalid_types:
        with pytest.raises(CondaException) as exc_info:
            Resolver.get_resolver(invalid_type)
        assert "does not exist" in str(exc_info.value)


# ===== get_resolver_cls tests =====
test_cases = [
    (
        # env_desc
        {
            "env_type": EnvType.CONDA_ONLY,
        },
        # Expected Resolver cls
        CondaResolver,
        # Test Case Id
        "Conda Resolver by env_type, file_paths whole entry missing",
    ),
    (
        # env_desc
        {"env_type": EnvType.CONDA_ONLY, "file_paths": {}},
        # Expected Resolver cls
        CondaResolver,
        # Test Case Id
        "Conda Resolver by env_type, empty file_paths dict",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.CONDA_ONLY,
            "file_paths": {"a": ["b"]},
        },
        # Expected Resolver cls
        CondaResolver,
        # Test Case Id
        "Conda Resolver by env_type, file_paths dict doesn't contain pylocK_toml entry",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.CONDA_ONLY,
            "file_paths": {"pylock_toml": []},
        },
        # Expected Resolver cls
        CondaResolver,
        # Test Case Id
        "Conda Resolver by env_type, file_paths dict's pylock_toml entry is empty list",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.CONDA_ONLY,
            "file_paths": {"pylock_toml": ["some_path"]},
        },
        # Expected Resolver cls
        PylockTomlResolver,
        # Test Case Id
        "Conda Resolver by env_type, file_paths dict's pylock_toml entry is not empty list",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.PYPI_ONLY,
            "file_paths": {},
        },
        # Expected Resolver cls
        PipResolver,
        # Test Case Id
        "PYPI_ONLY env_type, file_paths dict's pylock_toml entry is empty list",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.MIXED,
            "file_paths": {},
        },
        # Expected Resolver cls
        CondaLockResolver,
        # Test Case Id
        "Mixed env_type, file_paths dict's pylock_toml entry is empty list",
    ),
    (
        # env_desc
        {
            "env_type": EnvType.MIXED,
            "file_paths": {"pylock_toml": ["some_path1", "some_path2"]},
        },
        # Expected Resolver cls
        PylockTomlResolver,
        # Test Case Id
        "Mixed by env_type, file_paths dict's pylock_toml entry is not empty list",
    ),
]


@pytest.mark.parametrize(
    "env_desc, expected_resolver_cls, _test_id_",
    test_cases,
    ids=[tuple[2] for tuple in test_cases],
)
def test_get_resolver(env_desc, expected_resolver_cls, _test_id_):
    cls = EnvsResolver.get_resolver_cls(env_desc)
    assert cls == expected_resolver_cls
