from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.json_function import JsonFunction
from taskless_fn import predict

params = FunctionParameters(multiplier=3, label="test")
fn = JsonFunction(predict, params=params)
print("Reference:", fn.reference)