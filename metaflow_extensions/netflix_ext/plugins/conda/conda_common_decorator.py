# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
import copy

from itertools import chain
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK

from metaflow._vendor.packaging.utils import canonicalize_version

from .utils import merge_dep_dicts


class StepRequirementIface:
    @property
    def from_name(self) -> Optional[str]:
        return None

    @property
    def from_pathspec(self) -> Optional[str]:
        return None

    @property
    def python(self) -> Optional[str]:
        return None

    @property
    def is_fetch_at_exec(self) -> Optional[bool]:
        return None

    @property
    def is_disabled(self) -> Optional[bool]:
        return None

    @property
    def packages(self) -> Dict[str, Dict[str, str]]:
        return {}

    @property
    def sources(self) -> Dict[str, List[str]]:
        return {}

    def default_disabled(self, ubf_context: str) -> Optional[bool]:
        return None


class StepRequirementMixin(StepRequirementIface):
    defaults = {
        "disabled": None,
    }  # type: Dict[str, Any]

    @property
    def is_disabled(self) -> Optional[bool]:
        if self.attributes["disabled"] is None:
            if (
                self.python
                or self.from_name
                or self.from_pathspec
                or self.packages
                or self.sources
            ):
                return False
            return None
        return self.attributes["disabled"]


class StepRequirement(StepRequirementIface):
    def __init__(self):
        self._name = None  # type: Optional[str]
        self._pathspec = None  # type: Optional[str]
        self._python = None  # type: Optional[str]
        self._fetch_at_exec = None  # type: Optional[bool]
        self._disabled = None  # type: Optional[bool]
        self._packages = {}  # type: Dict[str, Dict[str, str]]
        self._sources = {}  # type: Dict[str, List[str]]
        self._default_disabled = {
            UBF_CONTROL: None,
            UBF_TASK: None,
        }  # type: Dict[str, Optional[bool]]

    def copy(self) -> "StepRequirement":
        n = StepRequirement()
        n._name = self._name
        n._pathspec = self._pathspec
        n._python = self._python
        n._fetch_at_exec = self._fetch_at_exec
        n._disabled = self._disabled
        n._packages = copy.deepcopy(self._packages)
        n._sources = copy.deepcopy(self._sources)
        n._default_disabled = copy.deepcopy(self._default_disabled)
        return n

    @property
    def from_name(self) -> Optional[str]:
        return self._name

    @property
    def from_pathspec(self) -> Optional[str]:
        return self._pathspec

    @property
    def from_env_name(self) -> Optional[str]:
        if self.from_name:
            return self.from_name
        elif self.from_pathspec:
            return "step:%s" % self.from_pathspec

    @property
    def python(self) -> Optional[str]:
        return self._python

    @python.setter
    def python(self, value: str):
        self._python = value

    @property
    def is_fetch_at_exec(self) -> Optional[bool]:
        return self._fetch_at_exec

    @property
    def is_disabled(self) -> Optional[bool]:
        if self._disabled is None:
            if (
                self.python
                or self.from_name
                or self.from_pathspec
                or self.packages
                or self.sources
            ):
                return False
            return None
        return self._disabled

    @property
    def packages(self) -> Dict[str, Dict[str, str]]:
        return copy.deepcopy(self._packages)

    @property
    def packages_as_str(self) -> Dict[str, List[str]]:
        result = {}  # type: Dict[str, List[str]]
        for category, values in self.packages.items():
            result[category] = [
                "%s==%s" % (k, v) if v else k for k, v in values.items()
            ]
        return result

    @packages.setter
    def packages(self, value: Dict[str, Dict[str, str]]):
        self._packages = value

    @property
    def sources(self) -> Dict[str, List[str]]:
        return copy.deepcopy(self._sources)

    @sources.setter
    def sources(self, value: Dict[str, List[str]]):
        self._sources = value

    def default_disabled(self, ubf_context: str) -> Optional[bool]:
        return self._default_disabled[ubf_context]

    def __repr__(self) -> str:
        disabled_part = (
            "disabled=%s" % self.is_disabled if self.is_disabled is not None else ""
        )
        if self.is_disabled:
            return "StepReq[%s]" % disabled_part
        python_part = "python=%s" % self.python if self.python else ""
        fetch_at_exec_part = (
            "fetch_at_exec=%s" % self.is_fetch_at_exec
            if self.is_fetch_at_exec is not None
            else ""
        )
        from_env_part = "from=%s" % self.from_env_name if self.from_env_name else ""
        packages_part = "packages=%s" % self.packages if self.packages else ""
        sources_part = "sources=%s" % self.sources if self.sources else ""

        return "StepReq[%s]" % "; ".join(
            filter(
                lambda x: x,
                [
                    disabled_part,
                    python_part,
                    fetch_at_exec_part,
                    from_env_part,
                    packages_part,
                    sources_part,
                ],
            )
        )

    def merge_update(self, other: StepRequirementIface):
        def _check_and_return(f: str, v1: Any, v2: Any) -> Any:
            if v1 is not None:
                if v2 is None or v1 == v2:
                    return v1
                raise InvalidEnvironmentException(
                    "%s is specified with incompatible values: %s and %s" % (f, v1, v2)
                )
            else:
                return v2

        self._internal_merge(other, _check_and_return, merge_dep_dicts)

    def override_update(self, other: StepRequirementIface):
        def _override_if_none(f: str, v1: Any, v2: Any) -> Any:
            if v2 is None:
                return v1
            return v2

        def _update_dict(d1: Dict[str, str], d2: Dict[str, str]) -> Dict[str, str]:
            d1.update(d2)
            return d1

        self._internal_merge(other, _override_if_none, _update_dict)

    def _internal_merge(
        self,
        other: StepRequirementIface,
        check_func: Callable[[str, Any, Any], Any],
        merge_func: Callable[[Dict[str, str], Dict[str, str]], Dict[str, str]],
    ):
        self._name = check_func("name", self.from_name, other.from_name)
        self._pathspec = check_func("pathspec", self.from_pathspec, other.from_pathspec)
        self._python = check_func("python", self.python, other.python)
        self._fetch_at_exec = check_func(
            "fetch_at_exec", self.is_fetch_at_exec, other.is_fetch_at_exec
        )
        self._disabled = check_func("disabled", self.is_disabled, other.is_disabled)
        for ubf_context in (UBF_CONTROL, UBF_TASK):
            self._default_disabled[ubf_context] = check_func(
                "default_disabled",
                self.default_disabled(ubf_context),
                other.default_disabled(ubf_context),
            )

        other_packages = other.packages

        for category, packages in other_packages.items():
            self._packages[category] = merge_func(
                self._packages.get(category, {}), packages
            )

        other_sources = other.sources
        for category, sources in other_sources.items():
            self._sources[category] = list(
                set(sources + self._sources.get(category, []))
            )

        # Special handling for pathspec/name
        if other.from_name is not None and other.from_pathspec is not None:
            raise InvalidEnvironmentException(
                "Cannot specify both `name` and `pathspec` in a decorator"
            )
        if other.from_name is not None:
            self._pathspec = None  # Use the latest and override any previous one
        elif other.from_pathspec is not None:
            self._name = None


class CondaRequirementDecoratorMixin(StepRequirementMixin):
    defaults = {
        "python": None,
        "libraries": {},
        "channels": [],
        # The next fields are deprecated in favor of @named_env and @pypi
        "pip_packages": {},
        "pip_sources": [],
        "name": None,
        "pathspec": None,
        "fetch_at_exec": None,
        **StepRequirementMixin.defaults,
    }

    @property
    def python(self) -> Optional[str]:
        return self.attributes["python"]

    @property
    def from_name(self) -> Optional[str]:
        return self.attributes["name"]

    @property
    def pathspec(self) -> Optional[str]:
        return self.attributes["pathspec"]

    @property
    def is_fetch_at_exec(self) -> Optional[bool]:
        return self.attributes["fetch_at_exec"]

    @property
    def packages(self) -> Dict[str, Dict[str, str]]:
        return {
            "conda": {
                k: v
                for k, v in cast(Dict[str, str], self.attributes["libraries"]).items()
            },
            "pypi": {
                k: canonicalize_version(v)
                for k, v in cast(
                    Dict[str, str], self.attributes["pip_packages"]
                ).items()
            },
        }

    @property
    def sources(self) -> Dict[str, List[str]]:
        return {
            "conda": [k for k in cast(List[str], self.attributes["channels"])],
            "pypi": [k for k in cast(List[str], self.attributes["pip_sources"])],
        }


class PypiRequirementDecoratorMixin(StepRequirementMixin):
    defaults = {
        "python": None,
        "packages": {},
        "extra_indices": [],
        # The next fields are deprecated in favor of @named_env
        "sources": [],
        "name": None,
        "pathspec": None,
        "fetch_at_exec": None,
        **StepRequirementMixin.defaults,
    }

    @property
    def python(self) -> Optional[str]:
        return self.attributes["python"]

    @property
    def from_name(self) -> Optional[str]:
        return self.attributes["name"]

    @property
    def pathspec(self) -> Optional[str]:
        return self.attributes["pathspec"]

    @property
    def is_fetch_at_exec(self) -> Optional[bool]:
        return self.attributes["fetch_at_exec"]

    @property
    def packages(self) -> Dict[str, Dict[str, str]]:
        return {
            "pypi": {
                k: canonicalize_version(v)
                for k, v in cast(Dict[str, str], self.attributes["packages"]).items()
            }
        }

    @property
    def sources(self) -> Dict[str, List[str]]:
        return {
            "pypi": [
                k
                for k in cast(
                    List[str],
                    chain(self.attributes["sources"], self.attributes["extra_indices"]),
                )
            ]
        }


class NamedEnvRequirementDecoratorMixin(StepRequirementMixin):
    defaults = {
        "name": None,
        "pathspec": None,
        "fetch_at_exec": None,
        **StepRequirementMixin.defaults,
    }

    @property
    def from_name(self) -> Optional[str]:
        return self.attributes["name"]

    @property
    def from_pathspec(self) -> Optional[str]:
        return self.attributes["pathspec"]

    @property
    def is_fetch_at_exec(self) -> Optional[bool]:
        return self.attributes["fetch_at_exec"]


class SysPackagesRequirementDecoratorMixin(StepRequirementMixin):
    defaults = {
        "packages": None,
        **StepRequirementMixin.defaults,
    }

    @property
    def packages(self) -> Dict[str, Dict[str, str]]:
        return {
            "sys": {
                k: v
                for k, v in cast(Dict[str, str], self.attributes["packages"]).items()
            }
        }
