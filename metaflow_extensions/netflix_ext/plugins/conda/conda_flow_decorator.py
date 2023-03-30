from metaflow.decorators import FlowDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException


class CondaFlowDecorator(FlowDecorator):
    """
    Specifies the Conda environment for all steps of the flow.

    Use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    name : Optional[str]
        If specified, can refer to a named environment. The environment referred to
        here will be the one used as a base environment for all steps.
        If specified, nothing else can be specified in this decorator
    pathspec : Optional[str]
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used as a base environment for all steps.
        If specified, nothing else can be specified in this decorator.
    libraries : Dict[str, str]
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`). Note that versions can
        be specified either as a specific version or as a comma separated string
        of constraints like "<2.0,>=1.5".
    channels : List[str]
        Additional channels to search
    pip_packages : Dict[str, str]
        Same as libraries but for pip packages
    pip_sources : List[str]
        Same as channels but for pip sources
    python : string
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda_base"
    defaults = {
        "name": None,
        "pathspec": None,
        "libraries": {},
        "channels": [],
        "pip_packages": {},
        "pip_sources": [],
        "python": None,
        "disabled": None,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if "pip_base" in flow._flow_decorators:
            raise InvalidEnvironmentException(
                "conda_base decorator is not compatible with pip_base. "
                "Please specify only one of them."
            )
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda_base* decorator requires " "--environment=conda"
            )

        if (self.attributes["name"] or self.attributes["pathspec"]) and len(
            [
                k
                for k, v in self.attributes.items()
                if v and k not in ("name", "pathspec")
            ]
        ):
            raise InvalidEnvironmentException(
                "You cannot specify `name` or `pathspec` along with other attributes in @%s"
                % self.name
            )


class PipFlowDecorator(FlowDecorator):
    """
    Specifies the Pip environment for all steps in the flow.

    Information in this decorator will augment any
    attributes set in the `@pip_base` flow-level decorator. Hence
    you can use `@pip_base` to set common libraries required by all
    steps and use `@pip` to specify step-specific additions.

    Parameters
    ----------
    name : Optional[str]
        If specified, can refer to a named environment. The environment referred to
        here will be the one used as a base environment for all steps.
        If specified, nothing else can be specified in this decorator
    pathspec : Optional[str]
        If specified, can refer to the pathspec of an existing step. The environment
        of this referred step will be used as a base environment for all steps.
        If specified, nothing else can be specified in this decorator.
    packages : Dict[str, str]
        Packages to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    sources : List[str]
        Additional channels to search for
    python : str
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Pip (default: False).
    """

    name = "pip_base"

    defaults = {
        "name": None,
        "pathspec": None,
        "packages": {},
        "sources": [],
        "python": None,
        "disabled": None,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if "conda_base" in flow._flow_decorators:
            raise InvalidEnvironmentException(
                "pip_base decorator is not compatible with conda_base. "
                "Please specify only one of them."
            )
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@pip_base* decorator requires --environment=conda"
            )

        if (self.attributes["name"] or self.attributes["pathspec"]) and len(
            [
                k
                for k, v in self.attributes.items()
                if v and k not in ("name", "pathspec")
            ]
        ):
            raise InvalidEnvironmentException(
                "You cannot specify `name` or `pathspec` along with other attributes in @%s"
                % self.name
            )
