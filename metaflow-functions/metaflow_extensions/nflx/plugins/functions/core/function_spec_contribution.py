"""Additive metadata contributions to a function specification."""

import json
from dataclasses import dataclass
from typing import Any, Dict, Tuple


_CONTRIBUTIONS_ATTRIBUTE = "_function_spec_metadata_contributions"
_METADATA_FIELDS = {"system_metadata", "user_metadata"}


@dataclass(frozen=True)
class FunctionSpecMetadataContribution:
    """A JSON-safe, namespaced contribution to a FunctionSpec metadata field."""

    field: str
    namespace: str
    metadata: Dict[str, Any]

    def __post_init__(self) -> None:
        if self.field not in _METADATA_FIELDS:
            raise ValueError(
                f"metadata contribution field must be one of {sorted(_METADATA_FIELDS)}"
            )
        if not isinstance(self.namespace, str) or not self.namespace.strip():
            raise TypeError(
                "metadata contribution namespace must be a non-empty string"
            )
        if not isinstance(self.metadata, dict):
            raise TypeError("metadata contribution must be a dict")

        try:
            normalized_metadata = json.loads(json.dumps(self.metadata, sort_keys=True))
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"metadata contribution must be JSON serializable: {e}"
            ) from e
        object.__setattr__(self, "metadata", normalized_metadata)


def add_function_spec_metadata(
    func: Any, contribution: FunctionSpecMetadataContribution
) -> Any:
    """
    Attach an additive metadata contribution to a callable.

    The callable is returned unchanged so metadata-only decorators can stack with
    function-type decorators without introducing another wrapper.
    """
    if not isinstance(contribution, FunctionSpecMetadataContribution):
        raise TypeError(
            "contribution must be a FunctionSpecMetadataContribution instance"
        )

    contributions = tuple(getattr(func, _CONTRIBUTIONS_ATTRIBUTE, ()))
    setattr(func, _CONTRIBUTIONS_ATTRIBUTE, contributions + (contribution,))
    return func


def get_function_spec_metadata(
    func: Any,
) -> Tuple[FunctionSpecMetadataContribution, ...]:
    """Return metadata contributions previously attached to a callable."""
    contributions = tuple(getattr(func, _CONTRIBUTIONS_ATTRIBUTE, ()))
    if not all(
        isinstance(item, FunctionSpecMetadataContribution) for item in contributions
    ):
        raise TypeError("callable has an invalid function spec metadata contribution")
    return contributions
