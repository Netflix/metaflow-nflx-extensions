"""
Pluggable HuggingFace authentication for the @huggingface decorator.

Implement HuggingFaceAuthProvider and register the implementation through
HUGGINGFACE_AUTH_PROVIDERS, a config mapping of provider name to import path.
"""

import abc
from typing import Optional


class HuggingFaceAuthProvider(abc.ABC):
    """
    Interface for pluggable HuggingFace authentication.

    Providers are selected by HUGGINGFACE_AUTH_PROVIDER and are discovered from
    the extension-local HUGGINGFACE_AUTH_PROVIDERS config mapping.
    """

    TYPE = None  # type: Optional[str]

    @abc.abstractmethod
    def get_token(self) -> Optional[str]:
        """
        Return the HuggingFace API token to use for this task, or None if no auth.
        """
        pass
