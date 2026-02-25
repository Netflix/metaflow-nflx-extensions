from metaflow import FlowSpec, conda, pypi, named_env, step


class MyTestFlow(FlowSpec):
    @conda(libraries={"pandas": ""})
    @step
    def start(self):
        self.next(self.a)

    @pypi(packages={"pandas": ""})
    @step
    def a(self):
        self.next(self.b)

    @conda(disabled=True)
    @step
    def b(self):
        self.next(self.c)

    @named_env(name="mlp/metaflow/romain/test_pandas_env")
    @step
    def c(self):
        self.next(self.end)

    @conda(disabled=True)
    @step
    def end(self):
        pass


if __name__ == "__main__":
    MyTestFlow()
