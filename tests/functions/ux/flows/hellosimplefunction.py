from metaflow import current, FlowSpec, step, conda, Flow


class HelloSimpleFunction(FlowSpec):
    @conda
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
        from function_module import avro_transform_string, json_process_object

        # Get the completed start task to bind functions from
        flow = Flow(current.flow_name)
        run = flow[current.run_id]
        start_task = run["start"].task

        # Bind functions using the completed start task
        self.avro_string_function = AvroFunction(avro_transform_string, task=start_task)
        self.json_object_function = JsonFunction(json_process_object, task=start_task)

        self.next(self.test_functions)

    @conda
    @step
    def test_functions(self):
        from metaflow_extensions.nflx.plugins.functions.core.function import (
            function_from_json,
            close_function,
        )

        # Test avro string function
        avro_func = function_from_json(self.avro_string_function.reference)
        input_str = "hello"
        result = avro_func(input_str)
        expected = f"HELLO_{self.suffix}"
        assert result == expected, f"Expected {expected}, got {result}"
        close_function(avro_func)

        # Test json object function
        json_func = function_from_json(self.json_object_function.reference)
        input_obj = {"value": 42, "name": "test"}
        result = json_func(input_obj)
        assert result["processed"] == True
        assert result["increment"] == self.increment
        assert result["value"] == 42
        assert result["name"] == "test"
        close_function(json_func)

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloSimpleFunction()
