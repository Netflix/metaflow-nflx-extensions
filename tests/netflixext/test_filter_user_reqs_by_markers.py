"""
Test suite for filter_user_reqs_by_markers function.

This test suite focuses on testing all code paths of the filter_user_reqs_by_markers
function, with special attention to conda package version specifiers including:
- Build strings (package==version=build_string)
- Channel information (channel::package==version)
- Double equals patterns (package==<=1.20.0, package==*)
- Python version with build strings
"""

import pytest
from metaflow._vendor.packaging.markers import default_environment
from metaflow.metaflow_environment import InvalidEnvironmentException

from metaflow_extensions.netflix_ext.plugins.conda.utils import filter_user_reqs_by_markers


@pytest.fixture
def mock_target_env():
    """Mock target environment for testing."""
    return {
        "implementation_name": "cpython",
        "implementation_version": "3.10.0",
        "os_name": "posix",
        "platform_machine": "x86_64",
        "platform_release": "5.10.0",
        "platform_system": "Linux",
        "platform_version": "#1 SMP",
        "python_full_version": "3.10.0",
        "platform_python_implementation": "CPython",
        "python_version": "3.10",
        "sys_platform": "linux",
    }


class TestBasicFunctionality:
    """Test basic filtering functionality."""

    def test_empty_deps(self):
        """Test with empty dependencies dictionary."""
        result = filter_user_reqs_by_markers({}, "python==3.10", "linux-64")
        assert result == {}

    def test_deps_without_markers(self):
        """Test that dependencies without markers are kept as-is."""
        deps = {
            "conda": ["numpy==1.20.0", "pandas==1.3.0"],
            "pypi": ["requests==2.26.0", "pytest==6.2.5"],
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result == deps

    def test_non_conda_pypi_types_unchanged(self):
        """Test that non-conda/pypi dependency types are kept unchanged."""
        deps = {
            "sys": ["__glibc==2.27=0", "__cuda==11.2=0"],
            "conda": ["numpy==1.20.0"],
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["sys"] == deps["sys"]
        assert result["conda"] == ["numpy==1.20.0"]


class TestCondaVersionSpecifiers:
    """Test conda-specific version specifiers including build strings."""

    def test_build_string_without_marker(self):
        """Test package with build string but no marker."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0",
                "scipy==1.7.0=py310h9876543_1",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == deps["conda"]

    def test_build_string_with_marker_true(self):
        """Test package with build string and marker that evaluates to True."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == ["numpy==1.20.0=py310h1234567_0"]

    def test_build_string_with_marker_false(self):
        """Test package with build string and marker that evaluates to False."""
        deps = {
            "conda": [
                "numpy==1.20.0=py38h1234567_0; python_version < '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == []

    def test_channel_prefix_without_marker(self):
        """Test package with channel prefix but no marker."""
        deps = {
            "conda": [
                "conda-forge::numpy==1.20.0",
                "defaults::pandas==1.3.0",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == deps["conda"]

    def test_channel_prefix_with_marker(self):
        """Test package with channel prefix and marker."""
        deps = {
            "conda": [
                "conda-forge::numpy==1.20.0; python_version >= '3.9'",
                "defaults::pandas==1.3.0; python_version < '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == ["conda-forge::numpy==1.20.0"]

    def test_channel_and_build_string_with_marker(self):
        """Test package with both channel prefix and build string with marker."""
        deps = {
            "conda": [
                "conda-forge::numpy==1.20.0=py310h1234567_0; python_version >= '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == ["conda-forge::numpy==1.20.0=py310h1234567_0"]

    def test_double_equal_star(self):
        """Test package with ==* pattern (any version)."""
        deps = {
            "conda": [
                "numpy==*",
                "pandas==*; python_version >= '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == ["numpy==*", "pandas==*"]

    def test_double_equal_comparison(self):
        """Test package with ==<comparison> pattern."""
        deps = {
            "conda": [
                "numpy==<=1.20.0",
                "pandas==>=1.3.0; python_version >= '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == ["numpy==<=1.20.0", "pandas==>=1.3.0"]

    def test_complex_conda_specifiers_mixed(self):
        """Test mix of complex conda version specifiers."""
        deps = {
            "conda": [
                "conda-forge::numpy==1.20.0=py310h1234567_0",
                "scipy==1.7.0=py310h9876543_1; python_version >= '3.9'",
                "pandas==*",
                "matplotlib==<=3.5.0; python_version < '3.11'",
                "defaults::scikit-learn==1.0.0; sys_platform == 'doesnotexist'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=None
        )
        # All should be kept except those with false markers
        assert "conda-forge::numpy==1.20.0=py310h1234567_0" in result["conda"]
        assert "scipy==1.7.0=py310h9876543_1" in result["conda"]
        assert "pandas==*" in result["conda"]

    def test_conda_with_version_range_and_markers(self):
        """Test conda packages with version ranges and markers."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.9'",
                "scipy==1.7.0=py310h9876543_1; python_version < '3.13'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")
        # Both should be kept as markers are non-ambiguous for the range
        assert "numpy==1.20.0=py310h1234567_0" in result["conda"]
        assert "scipy==1.7.0=py310h9876543_1" in result["conda"]

    def test_conda_build_string_with_ambiguous_marker(self):
        """Test conda packages with build strings and ambiguous markers."""
        deps = {
            "conda": [
                # Ambiguous for range 3.9-3.11: False for 3.9, True for 3.10+
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.10'",
            ]
        }
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")


class TestPythonVersionHandling:
    """Test various Python version specifications."""

    def test_specific_python_version(self):
        """Test with specific Python version (==3.10.0)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version == '3.10'",
                "pandas==1.3.0; python_version == '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10.0", "linux-64")
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_python_version_range(self):
        """Test with Python version range (>=3.9,<3.12) with non-ambiguous markers."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.9'",
                "scipy==1.7.0; python_version < '3.12'",
                "requests==2.26.0; python_version >= '3.8'",
                "matplotlib==3.5.0; python_version < '3.13'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")
        # All should be kept as they all evaluate to True for the entire range
        assert "numpy==1.20.0" in result["pypi"]
        assert "scipy==1.7.0" in result["pypi"]
        assert "requests==2.26.0" in result["pypi"]
        assert "matplotlib==3.5.0" in result["pypi"]

    def test_python_version_range_all_filtered(self):
        """Test version range where all deps are filtered out (consistent False)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version < '3.9'",
                "pandas==1.3.0; python_version >= '3.12'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")
        # All should be filtered out as they all evaluate to False for the entire range
        assert result["pypi"] == []

    def test_python_version_range_ambiguous_marker(self):
        """Test that version ranges with ambiguous markers raise exception."""
        deps = {
            "pypi": [
                # This is ambiguous: False for 3.9-3.10, True for 3.11
                "pandas==1.3.0; python_version >= '3.11'",
            ]
        }
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")

    def test_python_version_range_boundary_ambiguous(self):
        """Test boundary case where marker boundary is within range."""
        deps = {
            "pypi": [
                # Ambiguous: True for 3.10+, False for 3.9
                "numpy==1.20.0; python_version >= '3.10'",
            ]
        }
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")

    def test_python_version_range_less_than_ambiguous(self):
        """Test less-than marker that creates ambiguity within range."""
        deps = {
            "pypi": [
                # Ambiguous: True for 3.9-3.10, False for 3.11+
                "scipy==1.7.0; python_version < '3.11'",
            ]
        }
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")

    def test_python_version_range_narrow_non_ambiguous(self):
        """Test narrow range where marker spans entire range (non-ambiguous)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.10'",
                "pandas==1.3.0; python_version < '3.12'",
            ]
        }
        # Narrow range 3.10-3.11 makes both markers non-ambiguous
        result = filter_user_reqs_by_markers(deps, "python>=3.10,<3.11", "linux-64")
        assert "numpy==1.20.0" in result["pypi"]
        assert "pandas==1.3.0" in result["pypi"]

    def test_python_full_version_marker(self):
        """Test with python_full_version marker."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_full_version >= '3.10.0'",
                "pandas==1.3.0; python_full_version < '3.10.0'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10.5", "linux-64")
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_ambiguous_marker_raises_exception(self):
        """Test that ambiguous markers raise InvalidEnvironmentException."""
        deps = {
            "pypi": [
                # This marker evaluates differently for different patch versions
                # If we support 3.10.0-3.10.10, and marker is <= 3.10.5,
                # it would be ambiguous across the range
                "numpy==1.20.0; python_full_version <= '3.10.5'",
            ]
        }
        # When specifying a version range that could be ambiguous
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=3.10,<3.11", "linux-64")


class TestPythonVersionWithBuildString:
    """Test Python version parameter with conda build strings."""

    def test_python_version_with_build_string_simple(self):
        """Test Python version with build string (3.10.0=h1234567_0)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.9'",
                "pandas==1.3.0; python_version < '3.9'",
            ]
        }
        # Python version with build string should be parsed correctly
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.0=h1234567_0", "linux-64"
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_python_version_with_build_string_and_channel(self):
        """Test Python version with channel and build string."""
        deps = {
            "conda": [
                "numpy==1.20.0; python_version >= '3.10'",
                "pandas==1.3.0; python_version < '3.10'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "conda-forge::python==3.10.5=h9876543_1", "linux-64"
        )
        assert result["conda"] == ["numpy==1.20.0"]

    def test_python_version_with_complex_build_string(self):
        """Test Python version with complex conda build string."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version == '3.10'",
                "pandas==1.3.0; python_full_version >= '3.10.0'",
                "scipy==1.7.0; python_full_version < '3.11.0'",
            ]
        }
        # Complex build string with multiple segments
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.8=h257c98d_0_cpython", "linux-64"
        )
        assert "numpy==1.20.0" in result["pypi"]
        assert "pandas==1.3.0" in result["pypi"]
        assert "scipy==1.7.0" in result["pypi"]

    def test_python_version_with_build_string_full_version_marker(self):
        """Test Python version with build string against python_full_version marker."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_full_version >= '3.10.5'",
                "pandas==1.3.0; python_full_version < '3.10.5'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.8=h1234567_0", "linux-64"
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_python_version_range_with_build_string(self):
        """Test Python version range where one has build string."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.9'",
                "pandas==1.3.0; python_version >= '3.11'",
            ]
        }
        # Even with build string in range specifier, should work
        result = filter_user_reqs_by_markers(
            deps, "python>=3.9=h1111111_0,<3.11=h2222222_0", "linux-64"
        )
        assert "numpy==1.20.0" in result["pypi"]

    def test_python_exact_version_with_build_string(self):
        """Test exact Python version match with build string."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version == '3.10'",
                "pandas==1.3.0; python_version == '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.0=hab00c5b_1", "linux-64"
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_python_with_double_equal_and_build_string(self):
        """Test Python version with double equals pattern and build string."""
        deps = {
            "conda": [
                "numpy==1.20.0; python_version >= '3.10'",
            ]
        }
        # This tests that ==<version>=build works
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.0=h1234567_0", "linux-64"
        )
        assert result["conda"] == ["numpy==1.20.0"]

    def test_python_build_string_without_equals(self):
        """Test Python version with build string using dash separator."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.10'",
            ]
        }
        # Some conda packages might use dash instead of equals
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.0-h1234567_0", "linux-64"
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_mixed_python_and_deps_with_build_strings(self):
        """Test Python with build string and deps with build strings."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.10'",
                "scipy==1.7.0=py310h9876543_1; python_version >= '3.9'",
                "pandas==1.3.0=py39h1111111_0; python_version < '3.10'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.5=hab00c5b_2_cpython", "linux-64"
        )
        assert "numpy==1.20.0=py310h1234567_0" in result["conda"]
        assert "scipy==1.7.0=py310h9876543_1" in result["conda"]
        assert "pandas==1.3.0=py39h1111111_0" not in result["conda"]

    def test_python_version_no_patch_with_build_string(self):
        """Test Python version without patch number but with build string."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.10'",
            ]
        }
        # This should still work even though python version is not full
        result = filter_user_reqs_by_markers(
            deps, "python==3.10=h1234567_0", "linux-64"
        )
        assert result["pypi"] == ["numpy==1.20.0"]


class TestMarkerEvaluation:
    """Test marker evaluation with different conditions."""

    def test_platform_marker_linux(self, mock_target_env):
        """Test platform-specific markers for Linux."""
        deps = {
            "pypi": [
                "numpy==1.20.0; sys_platform == 'linux'",
                "pandas==1.3.0; sys_platform == 'darwin'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=mock_target_env
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_platform_marker_darwin(self):
        """Test platform-specific markers for macOS."""
        darwin_env = {
            "implementation_name": "cpython",
            "implementation_version": "3.10.0",
            "os_name": "posix",
            "platform_machine": "arm64",
            "platform_release": "21.0.0",
            "platform_system": "Darwin",
            "platform_version": "Darwin Kernel",
            "python_full_version": "3.10.0",
            "platform_python_implementation": "CPython",
            "python_version": "3.10",
            "sys_platform": "darwin",
        }
        deps = {
            "pypi": [
                "numpy==1.20.0; sys_platform == 'darwin'",
                "pandas==1.3.0; sys_platform == 'linux'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "osx-arm64", target_env=darwin_env
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_combined_markers(self, mock_target_env):
        """Test combined markers (and/or)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; sys_platform == 'linux' and python_version >= '3.9'",
                "pandas==1.3.0; sys_platform == 'darwin' or python_version < '3.9'",
                "scipy==1.7.0; sys_platform == 'linux' and python_version < '3.12'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=mock_target_env
        )
        # numpy and scipy should match (linux and python conditions met)
        # pandas should not match (darwin is false, python < 3.9 is false)
        assert "numpy==1.20.0" in result["pypi"]
        assert "scipy==1.7.0" in result["pypi"]
        assert "pandas==1.3.0" not in result["pypi"]

    def test_marker_with_platform_machine(self, mock_target_env):
        """Test markers with platform_machine."""
        deps = {
            "pypi": [
                "numpy==1.20.0; platform_machine == 'x86_64'",
                "pandas==1.3.0; platform_machine == 'arm64'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=mock_target_env
        )
        assert result["pypi"] == ["numpy==1.20.0"]


class TestMixedDependencyTypes:
    """Test with mixed dependency types."""

    def test_all_dependency_types(self, mock_target_env):
        """Test with conda, pypi, and other dependency types together."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.9'",
                "pandas==1.3.0; python_version < '3.9'",
            ],
            "pypi": [
                "requests==2.26.0",
                "pytest==6.2.5; python_version >= '3.9'",
            ],
            "sys": ["__glibc==2.27=0", "__cuda==11.2=0"],
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=mock_target_env
        )
        assert result["conda"] == ["numpy==1.20.0=py310h1234567_0"]
        assert result["pypi"] == ["requests==2.26.0", "pytest==6.2.5"]
        assert result["sys"] == ["__glibc==2.27=0", "__cuda==11.2=0"]

    def test_empty_filtered_results(self):
        """Test when all dependencies are filtered out."""
        deps = {
            "conda": [
                "numpy==1.20.0; python_version < '3.9'",
            ],
            "pypi": [
                "pandas==1.3.0; python_version < '3.9'",
            ],
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == []
        assert result["pypi"] == []


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_no_semicolon_fast_path(self):
        """Test the fast path for dependencies without semicolons."""
        deps = {
            "conda": [
                "numpy==1.20.0",
                "_libgcc_mutex==0.1",  # Starts with underscore, not valid Requirement
                "conda-forge::pandas==1.3.0=py310h1234567_0",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # All should be kept since there are no markers
        assert result["conda"] == deps["conda"]

    def test_underscore_package_name(self):
        """Test package names starting with underscore (conda-specific)."""
        deps = {
            "conda": [
                "_libgcc_mutex==0.1",
                "_openmp_mutex==4.5",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["conda"] == deps["conda"]

    def test_complex_build_strings(self):
        """Test various complex build string patterns."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h5f9c6_0",
                "scipy==1.7.0=py310hea3c10f_0",
                "pandas==1.3.0=py310h1234567_1",
                "matplotlib==3.5.0=py310h9876543_100",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert len(result["conda"]) == 4
        assert all(dep in result["conda"] for dep in deps["conda"])

    def test_marker_with_extra_whitespace(self):
        """Test markers with extra whitespace."""
        deps = {
            "pypi": [
                "numpy==1.20.0  ;  python_version >= '3.9'  ",
                "pandas==1.3.0;python_version<'3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["pypi"] == ["numpy==1.20.0  "]

    def test_multiple_version_constraints(self):
        """Test packages with multiple version constraints."""
        deps = {
            "pypi": [
                "numpy>=1.19.0,<1.21.0",
                "pandas>=1.2.0,<2.0.0; python_version >= '3.9'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["pypi"] == ["numpy>=1.19.0,<1.21.0", "pandas>=1.2.0,<2.0.0"]

    def test_invalid_python_version_format(self):
        """Test with invalid Python version format that doesn't match known versions."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.9'",
            ]
        }
        # Should raise exception when version doesn't match known versions
        # and doesn't start with "=="
        with pytest.raises(InvalidEnvironmentException):
            filter_user_reqs_by_markers(deps, "python>=999.0", "linux-64")

    def test_url_dependencies(self):
        """Test with URL-based dependencies."""
        deps = {
            "pypi": [
                "package @ https://example.com/package-1.0.0.tar.gz",
                "another @ git+https://github.com/user/repo.git",
            ]
        }
        # These should pass through since they have no markers
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert result["pypi"] == deps["pypi"]

    def test_python_with_build_string_no_semicolon_fast_path(self):
        """Test fast path (no semicolon) with Python having build string."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0",
                "_libgcc_mutex==0.1",
            ]
        }
        # Python with build string, but deps have no markers
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.5=hab00c5b_2", "linux-64"
        )
        # Should use fast path and keep all
        assert result["conda"] == deps["conda"]

    def test_version_with_local_identifier_and_marker(self):
        """Test version with local identifier (e.g., 1.2.3+local) and marker."""
        deps = {
            "pypi": [
                "package==1.2.3+local123; python_version >= '3.9'",
                "another==2.0.0.dev4+abc123; python_version < '3.12'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")
        # Both should be kept (non-ambiguous for range)
        assert "package==1.2.3+local123" in result["pypi"]
        assert "another==2.0.0.dev4+abc123" in result["pypi"]

    def test_complex_version_specifier_with_commas_in_range(self):
        """Test packages with complex version constraints and Python version ranges."""
        deps = {
            "pypi": [
                "numpy>=1.19.0,<1.21.0; python_version >= '3.9'",
                "pandas>=1.2.0,!=1.2.5,<2.0.0; python_version < '3.13'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python>=3.9,<3.12", "linux-64")
        assert "numpy>=1.19.0,<1.21.0" in result["pypi"]
        assert "pandas>=1.2.0,!=1.2.5,<2.0.0" in result["pypi"]

    def test_build_string_with_dash_separator_and_marker(self):
        """Test build strings with dash separator and markers."""
        deps = {
            "conda": [
                "numpy==1.20.0-py310h1234567_0; python_version >= '3.10'",
                "scipy==1.7.0-py39h9876543_1; python_version < '3.10'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        assert "numpy==1.20.0-py310h1234567_0" in result["conda"]
        assert "scipy==1.7.0-py39h9876543_1" not in result["conda"]

    def test_mixed_separators_build_strings(self):
        """Test conda packages with both = and - in build strings."""
        deps = {
            "conda": [
                "package==1.0.0=py310h123_0",
                "another==2.0.0-py310h456_1",
                "third==3.0.0=cpython_h789_2",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # All should be kept (no markers)
        assert len(result["conda"]) == 3


class TestArchitectureSpecific:
    """Test architecture-specific filtering."""

    def test_linux_64_arch(self, mock_target_env):
        """Test with linux-64 architecture."""
        deps = {
            "pypi": [
                "numpy==1.20.0; sys_platform == 'linux'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "linux-64", target_env=mock_target_env
        )
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_osx_arm64_arch(self):
        """Test with osx-arm64 architecture."""
        osx_env = {
            "implementation_name": "cpython",
            "implementation_version": "3.10.0",
            "os_name": "posix",
            "platform_machine": "arm64",
            "platform_release": "21.0.0",
            "platform_system": "Darwin",
            "platform_version": "Darwin Kernel",
            "python_full_version": "3.10.0",
            "platform_python_implementation": "CPython",
            "python_version": "3.10",
            "sys_platform": "darwin",
        }
        deps = {
            "pypi": [
                "numpy==1.20.0; sys_platform == 'darwin'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10", "osx-arm64", target_env=osx_env
        )
        assert result["pypi"] == ["numpy==1.20.0"]


class TestReturnedDependencyFormat:
    """Test that filtered dependencies maintain correct format."""

    def test_marker_removed_from_kept_deps(self):
        """Test that markers are removed from kept dependencies."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.9'",
                "pandas==1.3.0; python_version >= '3.10'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # Markers should be stripped from the results
        assert result["pypi"] == ["numpy==1.20.0", "pandas==1.3.0"]
        # Ensure no semicolons remain
        assert all(";" not in dep for dep in result["pypi"])

    def test_original_format_preserved_without_markers(self):
        """Test that original format is preserved when no markers exist."""
        deps = {
            "conda": [
                "conda-forge::numpy==1.20.0=py310h1234567_0",
                "defaults::pandas==1.3.0",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # Should preserve exact original format
        assert result["conda"] == deps["conda"]

    def test_marker_removed_with_python_build_string(self):
        """Test marker removal when Python version has build string."""
        deps = {
            "conda": [
                "numpy==1.20.0=py310h1234567_0; python_version >= '3.9'",
                "pandas==1.3.0; python_version >= '3.10'",
            ]
        }
        result = filter_user_reqs_by_markers(
            deps, "python==3.10.5=hab00c5b_2", "linux-64"
        )
        # Markers should be stripped from the results
        assert result["conda"] == [
            "numpy==1.20.0=py310h1234567_0",
            "pandas==1.3.0",
        ]
        # Ensure no semicolons remain
        assert all(";" not in dep for dep in result["conda"])


class TestCodePathCoverage:
    """Tests specifically designed to cover all code paths in the function."""

    def test_acceptable_versions_empty_specific_version(self):
        """Test code path where acceptable_versions is empty and specific version used."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version == '3.10'",
            ]
        }
        # Use exact version that doesn't match the filter list
        result = filter_user_reqs_by_markers(deps, "python==3.10.0", "linux-64")
        assert result["pypi"] == ["numpy==1.20.0"]

    def test_acceptable_versions_not_empty_with_range(self):
        """Test code path where acceptable_versions is not empty (range case)."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.10'",
            ]
        }
        # Use range that matches multiple versions in the filter list
        result = filter_user_reqs_by_markers(deps, "python>=3.10,<3.12", "linux-64")
        assert "numpy==1.20.0" in result["pypi"]

    def test_dep_type_not_conda_or_pypi(self):
        """Test code path for dependency types that are not conda or pypi."""
        deps = {
            "sys": ["__glibc==2.27=0"],
            "other": ["some-package==1.0.0"],
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # Both should be kept unchanged
        assert result["sys"] == deps["sys"]
        assert result["other"] == deps["other"]

    def test_no_semicolon_continue_path(self):
        """Test the continue path when there's no semicolon in dependency."""
        deps = {
            "conda": [
                "numpy==1.20.0",
                "pandas==1.3.0",
            ],
            "pypi": [
                "requests==2.26.0",
            ],
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # All should be kept via the fast path (no semicolon)
        assert result["conda"] == deps["conda"]
        assert result["pypi"] == deps["pypi"]

    def test_marker_false_continue_path(self):
        """Test the continue path when marker evaluates to False."""
        deps = {
            "pypi": [
                "numpy==1.20.0; python_version >= '3.11'",
                "pandas==1.3.0; sys_platform == 'win32'",
            ]
        }
        result = filter_user_reqs_by_markers(deps, "python==3.10", "linux-64")
        # Both should be filtered out
        assert result["pypi"] == []
