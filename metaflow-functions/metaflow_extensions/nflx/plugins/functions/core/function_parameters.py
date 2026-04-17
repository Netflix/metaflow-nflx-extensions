from __future__ import annotations

import types
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, TypeVar, Generic

if TYPE_CHECKING:
    from metaflow import Task


class LazyArtifactMapping(Mapping[str, Any]):
    def __init__(
        self,
        task: Optional[Task],
        function_spec: Optional["FunctionSpec"],  # type: ignore
        overrides: Dict[str, Any],
        expected_artifacts: Optional[set] = None,
    ) -> None:
        self._task: Optional[Task] = task
        self._function_spec = function_spec
        self._cache: Dict[str, Any] = dict(overrides)  # Start with overrides
        self._expected_artifacts: Optional[set] = expected_artifacts

    def __getitem__(self, key: str) -> Any:
        # Check cache first (includes overrides)
        if key in self._cache:
            return self._cache[key]

        # If we have expected artifacts specified, validate the key is expected for task artifacts
        if self._expected_artifacts is not None and key not in self._expected_artifacts:
            raise KeyError(
                f"Artifact '{key}' is not specified in the typed function parameters class"
            )

        if self._function_spec is not None:
            # If function_spec_params is not none, then we will raise an exception
            # if we cannot find the task here
            from metaflow import DataArtifact

            try:
                function_spec_params = self._function_spec.artifacts
                da = DataArtifact(
                    _object=function_spec_params[key],
                    pathspec=f"{self._function_spec.task_pathspec}/{key}",
                    _namespace_check=False,
                )
            except KeyError:
                raise KeyError(f"Artifact '{key}' is not found in FunctionParameters")

            data = da.data
            self._cache[key] = data
            return data

        if self._task is not None:
            try:
                artifact = self._task[key]
            except KeyError:
                raise KeyError(f"Artifact '{key}' not found in FunctionParameters.")
            data = artifact.data
            self._cache[key] = data
            return data
        raise KeyError(f"Artifact '{key}' not found in FunctionParameters.")

    def __iter__(self) -> Iterator[str]:
        # If expected artifacts are specified, iterate only over those
        if self._expected_artifacts is not None:
            yield from self._expected_artifacts
            return

        # Otherwise, use the original behavior
        seen = set(self._cache)
        yield from seen
        if self._task is not None:
            artifact_names = getattr(
                self._task.artifacts, "_fields", self._task.artifacts
            )
            for name in artifact_names:
                if name not in seen:
                    yield name

    def __len__(self) -> int:
        if self._expected_artifacts is not None:
            return len(self._expected_artifacts)

        keys = set(self._cache)
        if self._task is not None:
            artifact_names = getattr(
                self._task.artifacts, "_fields", self._task.artifacts
            )
            keys |= set(artifact_names)
        return len(keys)


T_FP_co = TypeVar("T_FP_co", covariant=True)
T_FP = TypeVar("T_FP", bound="FunctionParameters")


class FunctionParameters(Generic[T_FP_co]):
    """
    Container for data artifacts produced by a `Task` for use in
    `metaflow.functions`. A `FunctionParameters` object can also be used for
    testing functions before calling `bind` with a `Task`.

    Artifacts are accessed directly through item notation (e.g., `fd["artifact_name"]`)
    or attribute notation (e.g., `fd.artifact_name`).

    If a Task is provided, artifacts are retrieved from the Task unless overridden
    by keyword arguments.

    For typed artifact specification, subclass FunctionParameters with type annotations:

    class MyArtifacts(FunctionParameters):
        model: Any
        features: pd.DataFrame
        config: dict

    This will validate that the task contains the required artifacts at bind time
    and only preload the specified artifacts for efficiency.
    """

    _expected_artifacts: Optional[set] = None

    def __init__(
        self,
        task: Optional[Task] = None,
        prefetch_artifacts: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the FunctionParameters object.

        Parameters
        ----------
        task : Optional[Task], default None
            The Task object from which to retrieve artifacts. If None, no artifacts are retrieved.
        prefetch_artifacts : bool, default False
            If True, pre-fetch all artifacts from S3 during initialization to populate the cache.
            This improves first execute() call latency at the cost of slower initialization.
            If any artifact fails to download, initialization will fail with an exception.
        **kwargs : Any
            Keyword arguments representing artifact overrides. These will take precedence
            over artifacts retrieved from the Task.
        """
        # Validate task has required artifacts at bind time (for typed subclasses)
        # If function_spec is provided, we will fetch artifact values from S3 directly, instead of accessing the
        # task artifacts
        function_spec = kwargs.pop("function_spec", None)
        if task is not None and function_spec is not None:
            raise ValueError(
                f"Cannot specify both 'task' and 'function_spec' when initializing FunctionParameters."
            )
        if task is not None and self._expected_artifacts is not None:
            self._validate_task_artifacts(task, kwargs)

        data = LazyArtifactMapping(
            task, function_spec, kwargs, self._expected_artifacts
        )
        immutable_data: types.MappingProxyType[str, Any] = types.MappingProxyType(data)
        object.__setattr__(self, "_data", immutable_data)
        object.__setattr__(self, "_lazy_mapping", data)
        object.__setattr__(self, "_frozen", True)

        # Pre-fetch artifacts if requested
        if prefetch_artifacts:
            self._prefetch_all_artifacts(function_spec)

    def __init_subclass__(cls, **kwargs):
        """
        Called when FunctionParameters is subclassed. Extracts expected artifacts
        from type annotations.
        """
        super().__init_subclass__(**kwargs)

        # Get type annotations for this class AND inherited ones
        expected_artifacts = set()

        # Collect annotations from all classes in the MRO (method resolution order)
        for base_cls in reversed(cls.__mro__):
            if hasattr(base_cls, "__annotations__"):
                annotations = base_cls.__annotations__
                for field_name, field_type in annotations.items():
                    if field_name.startswith("_"):
                        continue
                    expected_artifacts.add(field_name)

        # Set expected artifacts on the class
        cls._expected_artifacts = expected_artifacts if expected_artifacts else None

    def _validate_task_artifacts(self, task: Task, overrides: Dict[str, Any]) -> None:
        """
        Validate that the task contains all required artifacts with correct types.

        Parameters
        ----------
        task : Task
            The task to validate
        overrides : Dict[str, Any]
            Override values that don't need to be in the task

        Raises
        ------
        KeyError
            If required artifacts are missing from the task
        TypeError
            If artifacts have wrong types
        """
        if self._expected_artifacts is None:
            return

        missing_artifacts = []
        type_mismatches = []

        # Get type annotations for validation
        type_annotations = {}
        for base_cls in reversed(self.__class__.__mro__):
            if hasattr(base_cls, "__annotations__"):
                type_annotations.update(base_cls.__annotations__)

        for artifact_name in self._expected_artifacts:
            # Skip if provided in overrides - overrides are not type-checked at bind time
            if artifact_name in overrides:
                continue

            # Check if artifact exists in task
            artifact_names = getattr(task.artifacts, "_fields", task.artifacts)
            if artifact_name not in artifact_names:
                missing_artifacts.append(artifact_name)
                continue

            # Type validation - get the actual data and check its type
            expected_type = type_annotations.get(artifact_name)
            if expected_type is not None:
                actual_data = task[artifact_name].data
                if not self._is_compatible_type(actual_data, expected_type):
                    type_mismatches.append(
                        f"{artifact_name}: expected {expected_type}, got {type(actual_data)}"
                    )

        if missing_artifacts:
            raise KeyError(
                f"Task is missing required artifacts for {self.__class__.__name__}: {missing_artifacts}"
            )

        if type_mismatches:
            raise TypeError(
                f"Task artifacts have wrong types for {self.__class__.__name__}: {type_mismatches}"
            )

    def _is_compatible_type(self, actual_data: Any, expected_type: Any) -> bool:
        """
        Check if actual data is compatible with expected type annotation.

        This is a basic type checker that handles common cases.
        """
        import typing

        # Handle Any type - always accepts anything
        if expected_type is typing.Any:
            return True

        # Handle basic types
        if expected_type in (int, str, float, bool, dict, list, tuple, set):
            return isinstance(actual_data, expected_type)

        # Handle typing generics like List[str], Dict[str, int], etc.
        origin = typing.get_origin(expected_type)
        if origin is not None:
            if not isinstance(actual_data, origin):
                return False

            type_args = typing.get_args(expected_type)
            if not type_args:
                return True

            if origin is list:
                return all(
                    self._is_compatible_type(item, type_args[0]) for item in actual_data
                )
            elif origin is dict and len(type_args) == 2:
                key_type, value_type = type_args
                # Special case: Dict[str, Any] should accept any dict with string keys
                if value_type is typing.Any:
                    return all(
                        self._is_compatible_type(k, key_type)
                        for k in actual_data.keys()
                    )
                return all(
                    self._is_compatible_type(k, key_type)
                    and self._is_compatible_type(v, value_type)
                    for k, v in actual_data.items()
                )
            else:
                return True

        # For other types (classes, etc.), try direct isinstance check
        try:
            return isinstance(actual_data, expected_type)
        except TypeError:
            # If isinstance fails (e.g., for complex generic types), fail validation
            return False

    def _prefetch_all_artifacts(self, function_spec) -> None:
        """
        Pre-fetch all artifacts from S3 to populate the cache.

        This method iterates through all artifacts defined in the function spec
        and accesses them via the lazy mapping, triggering S3 downloads. This
        improves first execute() call latency by ensuring all artifacts are cached
        during initialization.

        Parameters
        ----------
        function_spec : Optional[FunctionSpec]
            The function specification containing artifact metadata. If None,
            no pre-fetching is performed.

        Raises
        ------
        Exception
            If any artifact fails to download from S3. This is intentional -
            pre-fetch failures should fail fast during initialization rather
            than silently deferring errors to execute() time.
        """
        if function_spec is None or function_spec.artifacts is None:
            return

        artifact_names = list(function_spec.artifacts.keys())
        if not artifact_names:
            return

        from metaflow_extensions.nflx.plugins.functions.debug import debug

        debug.functions_exec(
            f"Pre-fetching {len(artifact_names)} artifacts: {artifact_names}"
        )

        for artifact_name in artifact_names:
            debug.functions_exec(f"Pre-fetching artifact: {artifact_name}")
            try:
                # Access the artifact via the lazy mapping to trigger S3 download
                # This will populate the cache in LazyArtifactMapping
                _ = self._lazy_mapping[artifact_name]
                debug.functions_exec(
                    f"Successfully pre-fetched artifact: {artifact_name}"
                )
            except Exception as e:
                debug.functions_exec(
                    f"Failed to pre-fetch artifact '{artifact_name}': {e}"
                )
                # Re-raise to fail fast - don't silently ignore errors
                raise

        debug.functions_exec(
            f"Artifact pre-fetch complete: {len(artifact_names)} artifacts cached"
        )

    @classmethod
    def get_expected_artifacts(cls) -> Optional[set]:
        """
        Get the set of expected artifact names for this class.

        Returns
        -------
        Optional[set]
            Set of expected artifact names, or None if not a typed subclass
        """
        return cls._expected_artifacts.copy() if cls._expected_artifacts else None

    @classmethod
    def validate_task_compatibility(cls, task: Task) -> bool:
        """
        Check if a task is compatible with this function parameters class.

        Parameters
        ----------
        task : Task
            The task to check

        Returns
        -------
        bool
            True if task has all required artifacts, False otherwise
        """
        try:
            cls(task=task)
            return True
        except KeyError:
            return False

    def __getitem__(self, name: str) -> Any:
        return self._data[name]

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"'FunctionParameters' object has no attribute '{name}'"
            )

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self, "_frozen") and self._frozen:
            raise AttributeError(
                "Cannot modify FunctionParameters after initialization."
            )
        object.__setattr__(self, name, value)

    def __setitem__(self, name: str, value: Any) -> None:
        raise TypeError("'FunctionParameters' object does not support item assignment")

    def __iter__(self):
        """Iterate over artifact names."""
        return iter(self._lazy_mapping)

    def __len__(self) -> int:
        """Return number of artifacts."""
        return len(self._lazy_mapping)

    def as_typed(self, typed_class: Type[T_FP]) -> T_FP:
        """
        Create a typed view of this FunctionParameters instance.

        The new instance is pre-populated from this instance's already-fetched
        cache, so previously prefetched artifacts are not re-fetched. Any
        artifacts not yet in the cache are loaded lazily from the same
        function spec.

        Parameters
        ----------
        typed_class : Type[FunctionParameters]
            A FunctionParameters subclass to instantiate.

        Returns
        -------
        FunctionParameters
            An instance of typed_class pre-populated from this instance's cache.
        """
        cached = dict(self._lazy_mapping._cache)
        return typed_class(
            function_spec=self._lazy_mapping._function_spec,
            **cached,
        )
