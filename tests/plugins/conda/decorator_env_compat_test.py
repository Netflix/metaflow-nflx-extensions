"""Regression: the @conda/@pypi step + flow decorators must accept any
conda-BASED environment (isinstance check), not only TYPE == "conda".

This is what lets `--environment=prebuilt` (PrebuiltCondaEnvironment, a
CondaEnvironment subclass with TYPE="prebuilt") use @pypi/@conda. The original
`environment.TYPE != "conda"` check rejected it outright, so the L3 build failed
at "The @pypi decorator requires --environment=conda".
"""

import metaflow  # noqa: F401  (import first; nflx plugins have a circular import)
import pytest
from unittest.mock import MagicMock

from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)
from metaflow_extensions.nflx.plugins.conda.conda_environment import CondaEnvironment
from metaflow_extensions.nflx.plugins.conda.conda_step_decorator import (
    PackageRequirementStepDecorator,
)


class _FakePrebuiltEnv(CondaEnvironment):
    # Mirrors PrebuiltCondaEnvironment: a conda env subclass with a non-"conda"
    # TYPE. The old `TYPE != "conda"` check rejected this; the isinstance check
    # accepts it.
    TYPE = "prebuilt"


class _FakeNflxEnv(MetaflowEnvironment):
    TYPE = "nflx"


def _step_init_with_env(environment):
    dec = PackageRequirementStepDecorator()
    m = MagicMock()
    # The base step_init only inspects `environment`; the rest are placeholders.
    dec.step_init(m, m, "start", [m], environment, m, m)


def test_decorator_accepts_conda_subclass_environment():
    # object.__new__ avoids the heavy CondaEnvironment constructor; the check
    # only needs an instance of the right type.
    _step_init_with_env(object.__new__(_FakePrebuiltEnv))  # must NOT raise


def test_decorator_accepts_plain_conda_environment():
    _step_init_with_env(object.__new__(CondaEnvironment))  # must NOT raise


def test_decorator_rejects_non_conda_environment():
    with pytest.raises(InvalidEnvironmentException):
        _step_init_with_env(object.__new__(_FakeNflxEnv))
