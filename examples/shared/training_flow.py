"""
Training flow — produces two relocatable Metaflow Functions:
  self.json_predict_fn   (dict -> dict)  used by FastAPI example
  self.avro_predict_fn   (str  -> str)   used by gRPC example

Run:
    python training_flow.py run

After a successful run, print the references with:
    python -c "
    from metaflow import Flow
    run = Flow('PredictionTrainingFlow').latest_run
    task = run['bind_functions'].task
    print('JSON ref:', task.data.json_predict_fn.reference)
    print('Avro ref:', task.data.avro_predict_fn.reference)
    "
"""

import os
import sys

from metaflow import FlowSpec, Flow, step, conda, current

# Keeps predict_fn importable from the current directory
sys.path.insert(0, os.path.dirname(__file__))

# Points Python two levels up to the root directory so it can find the 'metaflow_extensions' folder
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, repo_root)


class PredictionTrainingFlow(FlowSpec):

    @conda(libraries={"scikit-learn": "1.4.2", "numpy": "1.26.4"})
    @step
    def start(self):
        from sklearn.linear_model import LinearRegression
        import numpy as np

        # Synthetic 3-feature dataset: y = 1*x0 + 2*x1 - 0.5*x2 + noise
        rng = np.random.default_rng(42)
        X = rng.standard_normal((200, 3))
        y = X @ [1.0, 2.0, -0.5] + rng.normal(0, 0.05, 200)

        model = LinearRegression().fit(X, y)

        # Save the trained model weights as Metaflow artifacts.
        # These values are automatically available in predict_fn.py as params.coef and params.intercept
        # Important: the names here (coef, intercept) must exactly match what predict_fn.py reads.
        # If they don't match, the function will silently use fallback default values instead.
        self.coef = model.coef_.tolist()
        self.intercept = float(model.intercept_)

        print(f"Trained: coef={self.coef}, intercept={self.intercept:.4f}")
        self.next(self.bind_functions)

    @step
    def bind_functions(self):
        from metaflow_extensions.nflx.plugins.json_function import JsonFunction
        from metaflow_extensions.nflx.plugins.avro_function import AvroFunction
        from predict_fn import json_predict, avro_predict

        # We bind to the 'start' task (not this current step) because 'start' has the
        # @conda decorator which tells the system what packages the function needs.
        # Without @conda, the system doesn't know how to recreate the environment later.
        flow = Flow(current.flow_name)
        start_task = flow[current.run_id]["start"].task

        self.json_predict_fn = JsonFunction(json_predict, task=start_task)
        self.avro_predict_fn = AvroFunction(avro_predict, task=start_task)

        print("JSON ref:", self.json_predict_fn.reference)
        print("Avro ref:", self.avro_predict_fn.reference)

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    PredictionTrainingFlow()
