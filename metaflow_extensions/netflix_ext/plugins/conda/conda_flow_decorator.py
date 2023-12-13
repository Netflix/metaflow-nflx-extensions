# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

from metaflow.decorators import FlowDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException

from .conda_common_decorator import (
    CondaRequirementDecoratorMixin,
    NamedEnvRequirementDecoratorMixin,
    SysPackagesRequirementDecoratorMixin,
    PypiRequirementDecoratorMixin,
)


class PackageRequirementFlowDecorator(FlowDecorator):
    name = "package_base"

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *%s* decorator requires " "--environment=conda" % self.name
            )


class CondaRequirementFlowDecorator(
    CondaRequirementDecoratorMixin, PackageRequirementFlowDecorator
):
    """
    Specifies the Conda packages for all steps of the flow.

    Use `@conda_base`, `@pypi_base` or `@named_env_base` to set common libraries
    required by all steps and use `@conda` or `@pypi` to specify step-specific additions.

    Parameters
    ----------
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this flow. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(pathspec=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the pathspec, you can use `@{}` values and
        environment variables will be used to substitute.
    libraries : Dict[str, str], default {}
        Libraries to use for this flow. The key is the name of the package
        and the value is the version to use. Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : List[str], default []
        Additional channels to search
    pip_packages : Dict[str, str], default {}
        DEPRECATED -- use `@pypi(packages=)` instead.
        Same as libraries but for pip packages.
    pip_sources : List[str], default []
        DEPRECATED -- use `@pypi(extra_indices=)` instead.
        Same as channels but for pip sources.
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current version
        will be used.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(fetch_at_exec=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this flow to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "conda_base"

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        deprecated_keys = set(
            ("pip_packages", "pip_sources", "fetch_at_exec", "name", "pathspec")
        ).intersection((k for k, v in self.attributes.items() if v))

        if deprecated_keys:
            echo(
                "*DEPRECATED*: Using '%s' in '@%s' is deprecated. Please use '@pypi_base' "
                "or '@named_env_base' instead. "
                % (", ".join(deprecated_keys), self.name)
            )
        super().flow_init(
            flow, graph, environment, flow_datastore, metadata, logger, echo, options
        )


class PypiRequirementFlowDecorator(
    PypiRequirementDecoratorMixin, PackageRequirementFlowDecorator
):
    """
    Specifies the Pypi packages for all steps of the flow.

    Use `@conda_base`, `@pypi_base` or `@named_env_base` to set common libraries
    required by all steps and use `@conda` or `@pypi` to specify step-specific additions.

    Parameters
    ----------
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this flow. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    packages : Dict[str, str], default {}
        Packages to use for this flow. The key is the name of the package
        and the value is the version to use.
    extra_indices : List[str], default []
        Additional sources to search for
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current python
        version will be used.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(name=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this flow to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "pypi_base"

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        deprecated_keys = set(
            ("sources", "fetch_at_exec", "name", "pathspec")
        ).intersection((k for k, v in self.attributes.items() if v))

        if deprecated_keys:
            echo(
                "*DEPRECATED*: Using '%s' in '@%s' is deprecated. Please use "
                "'@named_env_base' instead. " % (", ".join(deprecated_keys), self.name)
            )

        return super().flow_init(
            flow, graph, environment, flow_datastore, metadata, logger, echo, options
        )


class NamedEnvRequirementFlowDecorator(
    NamedEnvRequirementDecoratorMixin, PackageRequirementFlowDecorator
):
    """
    Specifies a named environment to extract the environment from

    Use `@conda_base`, `@pypi_base` or `@named_env_base` to set common libraries
    required by all steps and use `@conda` or `@pypi` to specify step-specific additions.

    Parameters
    ----------
    name : str, optional, default None
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this flow. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    fetch_at_exec : bool, default False
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this flow to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "named_env_base"


class SysPackagesRequirementFlowDecorator(
    SysPackagesRequirementDecoratorMixin, PackageRequirementFlowDecorator
):
    """
    Specifies system virtual packages for this flow.

    This is an advanced usage decorator allowing you to override the __glibc and
    __cuda virtual packages that are used when resolving your environment.

    Parameters
    ----------
    packages : Dict[str, str], default {}
        System virtual packages to use for this flow. Supported keys are "__cuda" and
        "__glibc".
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "sys_packages_base"


class PipRequirementFlowDecorator(PypiRequirementFlowDecorator):
    """
    Specifies the Pypi packages for all steps of the flow.

    DEPRECATED: please use `@pypi_base` instead

    Parameters
    ----------
    name : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to a named environment. The environment referred to
        here will be the one used for this flow. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    pathspec : str, optional, default None
        DEPRECATED -- use `@named_env(name=)` instead.
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used here. If specified, nothing else can be
        specified in this decorator. In the name, you can use `@{}` values and
        environment variables will be used to substitute.
    packages : Dict[str, str], default {}
        Packages to use for this flow. The key is the name of the package
        and the value is the version to use.
    extra_indices : List[str], default []
        Additional sources to search for
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. If not specified, the current python
        version will be used.
    fetch_at_exec : bool, default False
        DEPRECATED -- use `@named_env(name=)` instead.
        If set to True, the environment will be fetched when the task is
        executing as opposed to at the beginning of the flow (or at deploy time if
        deploying to a scheduler). This option requires name or pathspec to be
        specified. This is useful, for example, if you want this flow to always use
        the latest named environment when it runs as opposed to the latest when it
        is deployed.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "pip_base"

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        echo("*DEPRECATED*: Use '@pypi_base' instead of '@%s'." % self.name)

        return super().flow_init(
            flow, graph, environment, flow_datastore, metadata, logger, echo, options
        )
