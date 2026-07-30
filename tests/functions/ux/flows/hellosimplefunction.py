from metaflow import current, FlowSpec, step, conda, Flow


class HelloSimpleFunction(FlowSpec):
    @conda(libraries={"pydash": "5.1.0"})
    @step
    def start(self):
        # Set parameters - these will be used by the functions
        self.suffix = "modified"
        self.multiplier = 3
        self.threshold = 5
        self.increment = 10

        self.next(self.bind_functions)

    @step
    def bind_functions(self):
        from metaflow_extensions.nflx.plugins.avro_function import AvroFunction
        from metaflow_extensions.nflx.plugins.json_function import JsonFunction
        from metaflow_extensions.nflx.plugins.functions.core.function_pipeline import (
            FunctionPipeline,
        )
        from function_module import (
            avro_simple_string,
            avro_pydash_string,
            avro_add_field,
            avro_double_values,
            avro_raise_user_error,
            json_simple_object,
        )

        # Get the completed start task to bind functions from
        flow = Flow(current.flow_name)
        run = flow[current.run_id]
        start_task = run["start"].task

        # Bind functions using the completed start task. Execution is left to
        # the caller (see tests/functions/ux/test_functions.py), which drives
        # the bound references directly, across backends.
        self.avro_simple_function = AvroFunction(avro_simple_string, task=start_task)
        self.avro_pydash_function = AvroFunction(avro_pydash_string, task=start_task)
        self.avro_error_function = AvroFunction(
            avro_raise_user_error, task=start_task
        )
        self.avro_pipeline_function = FunctionPipeline(
            functions=[
                AvroFunction(avro_add_field, task=start_task),
                AvroFunction(avro_double_values, task=start_task),
            ],
            name="avro_pipeline",
        )
        self.json_simple_function = JsonFunction(json_simple_object, task=start_task)

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloSimpleFunction()
