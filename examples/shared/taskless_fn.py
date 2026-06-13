from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.json_function import json_function

@json_function
def predict(data: dict, params: FunctionParameters) -> dict:
    features = data["features"]
    prediction = sum(features) * params.multiplier
    return {
        "prediction": prediction,
        "n_features": len(features)
    }