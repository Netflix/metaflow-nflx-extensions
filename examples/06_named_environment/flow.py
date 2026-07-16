from metaflow import FlowSpec, named_env, step


class NamedEnvFlow(FlowSpec):
    """
    Reference a previously resolved + aliased environment by name. The same
    alias is used for both steps so they share one cached environment.
    """

    @named_env(name="examples/named_demo")
    @step
    def start(self):
        import numpy as np

        print("start: numpy %s" % np.__version__)
        self.next(self.end)

    @named_env(name="examples/named_demo")
    @step
    def end(self):
        import numpy as np

        print("end: numpy %s" % np.__version__)


if __name__ == "__main__":
    NamedEnvFlow()
