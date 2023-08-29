# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type

if TYPE_CHECKING:
    from ..conda import Conda
    from ..env_descr import ResolvedEnvironment
    from ..env_descr import EnvType


from ..utils import CondaException


class Resolver:
    TYPES = ["invalid"]

    _class_per_type = None  # type: Optional[Dict[str, Type[Resolver]]]

    @classmethod
    def _ensure_class_per_type(cls):
        if cls._class_per_type is None:
            cls._class_per_type = {t: c for c in cls.__subclasses__() for t in c.TYPES}

    @classmethod
    def get_resolver(cls, resolver_type: str):
        cls._ensure_class_per_type()
        assert cls._class_per_type

        resolver = cls._class_per_type.get(resolver_type)
        if resolver is None:
            raise CondaException(
                "Dependency resolver '%s' does not exist" % resolver_type
            )
        return resolver

    def __init__(self, conda: Conda):
        self._conda = conda

    def resolve(
        self,
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        builder_envs: Optional[List[ResolvedEnvironment]] = None,
        base_env: Optional[ResolvedEnvironment] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        """
        Resolves the environment specified by the dependencies, the sources (channels
        or indices), extra information (used for Pypi resolvers) for the given
        architecture.

        The builder_envs are additional environments that may be used to help in
        the resolution.

        Parameters
        ----------
        deps : Dict[str, List[str]]
            Dependencies that need to be satisfied for this environment. The first key
            is the type of package (conda/pypi/etc)
        sources : Dict[str, List[str]]
            Sources to use (channels, indices, etc). The first key is the type of source
        extras : Dict[str, List[str]]
            Extra information (used by some resolver)
        architecture : str
            Architecture to resolve for
        builder_envs : Optional[List[ResolvedEnvironment]]
            Helper environments to use
        base_env : Optional[ResolvedEnvironment]
            Environment this environment is based off of (ie: it is being built on
            top of this environment).

        Returns
        -------
        Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]
            The first element of the tuple is this ResolvedEnvironment.
            The second element are the builder environments. This may
            return the same thing as the argument passed in or more environments if
            more were also built in the process.
        """
        raise NotImplementedError


# These need to be imported to "register"
from .builder_envs_resolver import BuilderEnvsResolver
from .conda_lock_resolver import CondaLockResolver
from .pip_resolver import PipResolver
