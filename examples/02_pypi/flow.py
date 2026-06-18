from metaflow import FlowSpec, pypi, step


class PypiFlow(FlowSpec):
    """Two steps, each pinning a different pandas version via @pypi."""

    @pypi(packages={"pandas": "2.1.4"}, python=">=3.10,<3.11")
    @step
    def start(self):
        import pandas as pd

        assert pd.__version__ == "2.1.4"
        print("start: pandas %s" % pd.__version__)
        self.next(self.end)

    @pypi(packages={"pandas": "2.2.2"}, python=">=3.10,<3.11")
    @step
    def end(self):
        import pandas as pd

        assert pd.__version__ == "2.2.2"
        print("end: pandas %s" % pd.__version__)


if __name__ == "__main__":
    PypiFlow()
