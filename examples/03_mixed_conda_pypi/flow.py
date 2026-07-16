from metaflow import FlowSpec, conda, conda_base, pypi, step


@conda_base(libraries={"numpy": "1.26.4"}, python=">=3.10,<3.11")
class MixedFlow(FlowSpec):
    """
    `@conda_base` sets defaults for the whole flow (numpy + python here).
    Individual steps can extend with `@conda` or override with `@pypi`.
    """

    @step
    def start(self):
        import numpy as np

        assert np.__version__ == "1.26.4"
        print("start: numpy %s (from @conda_base)" % np.__version__)
        self.next(self.conda_step, self.pypi_step)

    @conda(libraries={"scipy": "1.11.4"})
    @step
    def conda_step(self):
        import numpy as np
        import scipy

        print("conda_step: numpy=%s scipy=%s" % (np.__version__, scipy.__version__))
        self.next(self.join)

    @pypi(packages={"requests": "2.31.0"})
    @step
    def pypi_step(self):
        import requests

        print("pypi_step: requests=%s" % requests.__version__)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        print("done")


if __name__ == "__main__":
    MixedFlow()
