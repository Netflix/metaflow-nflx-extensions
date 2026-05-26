from metaflow import FunctionParameters
from metaflow_extensions.nflx.plugins.json_function import json_function
from metaflow_extensions.nflx.plugins.avro_function import avro_function


@json_function
def json_predict(
    data: dict, params: FunctionParameters = FunctionParameters()
) -> dict:
    """Predict with a linear regression model. Expects data = {"features": [f1, f2, ...]}.

    Task artifacts used: coef (list[float]), intercept (float).
    """
    import numpy as np

    coef = params.coef if hasattr(params, "coef") else [1.0]
    intercept = params.intercept if hasattr(params, "intercept") else 0.0

    X = np.array(data["features"], dtype=float)
    prediction = float(X @ coef + intercept)
    return {"prediction": prediction, "n_features": len(data["features"])}


@avro_function
def avro_predict(
    data: str, params: FunctionParameters = FunctionParameters()
) -> str:
    """Same model, string I/O (JSON-encoded). Used by gRPC example."""
    import json
    import numpy as np

    coef = params.coef if hasattr(params, "coef") else [1.0]
    intercept = params.intercept if hasattr(params, "intercept") else 0.0

    payload = json.loads(data)
    X = np.array(payload["features"], dtype=float)
    prediction = float(X @ coef + intercept)
    return json.dumps({"prediction": prediction})
