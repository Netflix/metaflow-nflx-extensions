from metaflow import FlowSpec, pypi, step


class PypiFlow(FlowSpec):
    """Two steps, each pinning a different pandas version via @pypi."""

    @pypi(packages={"pandas": "1.4.0"}, python=">=3.9,<3.10")
    @step
    def start(self):
        import pandas as pd

        assert pd.__version__ == "1.4.0"
        print("start: pandas %s" % pd.__version__)
        self.next(self.end)

    @pypi(packages={"pandas": "1.5.0"}, python=">=3.9,<3.10")
    @step
    def end(self):
        import pandas as pd

        assert pd.__version__ == "1.5.0"
        print("end: pandas %s" % pd.__version__)


if __name__ == "__main__":
    PypiFlow()
