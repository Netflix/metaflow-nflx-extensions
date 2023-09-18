# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false

from metaflow.decorators import FlowDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException

from .conda_common_decorator import (
    CondaRequirementDecoratorMixin,
    NamedEnvRequirementDecoratorMixin,
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
    # REC: I would just have my own doc string
    """
    Specifies the Conda packages for all steps of the flow.

    Use `@conda_base` or `@pypi_base` to set common libraries
    required by all steps and use `@conda` or `@pypi` to specify step-specific additions.

    Parameters
    ----------
    libraries : Optional[Dict[str, str]]
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`). Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : Optional[List[str]]
        Additional channels to search
    python : Optional[str]
        Version of Python to use, e.g. '3.7.4'. If not specified, the current version
        will be used.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "conda_base"


class PypiRequirementFlowDecorator(
    PypiRequirementDecoratorMixin, PackageRequirementFlowDecorator
):
    # REC: Same thing
    """
    Specifies the Pypi packages for all steps of the flow.

    Use `@conda_base` or `@pypi_base` to set common libraries
    required by all steps and use `@conda` or `@pypi` to specify step-specific additions.

    Parameters
    ----------
    packages : Optional[Dict[str, str]]
        Packages to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    extra_indices : Optional[List[str]]
        Additional sources to search for
    python : Optional[str]
        Version of Python to use, e.g. '3.7.4'. If not specified, the current python
        version will be used.
    disabled : bool, default False
        If set to True, uses the external environment.
    """

    name = "pypi_base"
