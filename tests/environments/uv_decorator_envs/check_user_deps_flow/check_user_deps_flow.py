from metaflow import FlowSpec, step, resolved_uv
from typing import List, Tuple, Optional


# In this flow, PylockTomlResolver.resolve() is overridden,
# and the passed in user_deps parameter is inspected to verify
# no extra user_deps is introduced.
@resolved_uv
class CheckUserDepsFlow(FlowSpec):
    EXPECTED_USER_DEPS = {"metaflow", "numpy", "python"}

    @step
    def start(self):
        self._verify_user_deps_in_pylock_toml_decorator(
            verifying_from_step="start", expected_user_deps=self.EXPECTED_USER_DEPS
        )
        self.next(self.verify)

    @step
    def verify(self):
        # numpy is added to uv.lock via "uv add numpy==2.2.6" command,
        # which updated pyproject.toml and uv.lock files accordingly.
        import numpy as np

        matrix = np.eye(3)
        print(f"Generated {matrix=} by calling numpy.")

        self._verify_user_deps_in_pylock_toml_decorator(
            verifying_from_step="verify", expected_user_deps=self.EXPECTED_USER_DEPS
        )

        self.next(self.end)

    @step
    def end(self):
        self._verify_user_deps_in_pylock_toml_decorator(
            verifying_from_step="end", expected_user_deps=self.EXPECTED_USER_DEPS
        )
        pass

    def _verify_user_deps_in_pylock_toml_decorator(
        self, verifying_from_step: str, expected_user_deps: set = None
    ):
        for step in self:
            for deco in step.decorators:
                if deco.name == "pylock_toml_internal":
                    user_deps = deco.attributes["user_deps_for_hash"]
                    print(
                        f"Verifying pylock_toml_internal's user_deps_for_hash at step '{step.name}',"
                        f" verification executed from step '{verifying_from_step}': {user_deps=}"
                    )
                    assert (
                        set(user_deps.keys()) == expected_user_deps
                    ), f"Unexpected user_deps found in pylock_toml_internal decorator at step '{step.name}': {user_deps.keys()}"
                    print("User deps verification passed.")


if __name__ == "__main__":
    CheckUserDepsFlow()
