"""Import interceptor for automatic serializer registration."""

import sys
import importlib
import importlib.abc
import importlib.machinery
import importlib.util
from typing import Dict, List, Tuple, Optional, Any, Set, Sequence
from types import ModuleType


class WrappedLoader(importlib.abc.Loader):
    """Wrapper around original loader that adds serializer registration."""

    def __init__(
        self,
        original_loader: importlib.abc.Loader,
        interceptor: "SerializerImportInterceptor",
    ):
        self.original_loader = original_loader
        self.interceptor = interceptor

    def create_module(
        self, spec: importlib.machinery.ModuleSpec
    ) -> Optional[ModuleType]:
        """Delegate to original loader."""
        return self.original_loader.create_module(spec)

    def exec_module(self, module: ModuleType) -> None:
        """Execute original loader first, then register serializers."""
        # Execute the original module first
        self.original_loader.exec_module(module)

        # Now do our serializer registration
        self.interceptor._register_serializers_for_module(module)


class SerializerImportInterceptor(importlib.abc.MetaPathFinder):
    """Import hook that automatically registers serializers when types are imported.

    This interceptor watches for specific modules to be imported and automatically
    registers serializers for types found in those modules. This eliminates the need
    for manual serializer registration scattered throughout the codebase.
    """

    def __init__(self) -> None:
        self._pending_registrations: Dict[str, List[Tuple[str, Any]]] = (
            {}
        )  # module_name -> list of (canonical_type, serializer_config)
        self._processed_modules: Set[str] = (
            set()
        )  # Prevents duplicate processing of the same module

    def register_type_for_module(
        self, module_name: str, canonical_type: str, serializer_config: Any
    ) -> None:
        """Register a type to watch for in a specific module."""
        if module_name not in self._pending_registrations:
            self._pending_registrations[module_name] = []
        self._pending_registrations[module_name].append(
            (canonical_type, serializer_config)
        )

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[ModuleType] = None,
    ) -> Optional[importlib.machinery.ModuleSpec]:
        """Check if we need to intercept this module import.

        Returns a modified spec that uses our wrapped loader if we're watching this module.
        """
        if fullname in self._pending_registrations:
            # Temporarily remove ourselves from meta_path to prevent infinite recursion
            # when we call importlib.util.find_spec() below
            was_in_meta_path = self in sys.meta_path
            if was_in_meta_path:
                sys.meta_path.remove(self)
            try:
                # Get the real spec
                spec = importlib.util.find_spec(fullname)
                if spec and spec.loader:
                    # Wrap the original loader instead of replacing it
                    spec.loader = WrappedLoader(spec.loader, self)
                    return spec
            finally:
                # Restore ourselves to meta_path
                if was_in_meta_path:
                    sys.meta_path.insert(0, self)
        return None

    def _register_serializers_for_module(self, module: ModuleType) -> None:
        """Register serializers for types found in the given module."""
        from metaflow_extensions.nflx.plugins.functions.debug import debug
        from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
            register_serializer_config,
        )

        module_name = module.__name__

        # Prevent duplicate processing of the same module
        if module_name in self._processed_modules:
            debug.functions_exec(
                f"SerializerImportInterceptor: Already processed {module_name}, skipping"
            )
            return

        debug.functions_exec(
            f"SerializerImportInterceptor: Processing {module_name} for serializer registration"
        )

        # Mark as processed to prevent duplicate registration
        self._processed_modules.add(module_name)

        # Register serializers for any types we find in this module
        if module_name in self._pending_registrations:
            for canonical_type, serializer_config in self._pending_registrations[
                module_name
            ]:
                # Extract class name from canonical type
                class_name = canonical_type.split(".")[-1]
                if hasattr(module, class_name):
                    debug.functions_exec(
                        f"SerializerImportInterceptor: Registering serializer for {canonical_type}"
                    )
                    register_serializer_config(serializer_config)
                else:
                    debug.functions_exec(
                        f"SerializerImportInterceptor: Class {class_name} not found in {module_name}"
                    )


# Global interceptor instance
_serializer_interceptor = SerializerImportInterceptor()


def _ensure_interceptor_installed() -> None:
    """Ensure the import interceptor is installed at the beginning of sys.meta_path."""
    if _serializer_interceptor in sys.meta_path:
        # Remove from current position if it exists
        sys.meta_path.remove(_serializer_interceptor)

    # Always insert at position 0 to ensure it runs first
    sys.meta_path.insert(0, _serializer_interceptor)


def register_serializer_for_type(canonical_type: str, serializer_config: Any) -> None:
    """Register a serializer to be installed when the type's module is imported.

    If the module is already imported, registers the serializer immediately.
    Otherwise, sets up an import interceptor to register it when the module is imported.
    """
    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        register_serializer_config,
    )
    from metaflow_extensions.nflx.plugins.functions.debug import debug

    module_name = ".".join(canonical_type.split(".")[:-1])  # Extract module name
    class_name = canonical_type.split(".")[-1]  # Extract class name

    # Check if the module is already imported
    if module_name in sys.modules:
        module = sys.modules[module_name]
        if hasattr(module, class_name):
            # Module already imported and has the class - register immediately
            debug.functions_exec(
                f"SerializerImportInterceptor: {canonical_type} module already imported, registering immediately"
            )
            register_serializer_config(serializer_config)
            return
        else:
            # Module imported but class not found - this is an error since we'll never get another chance
            from metaflow_extensions.nflx.plugins.functions.exceptions import (
                MetaflowFunctionException,
            )

            raise MetaflowFunctionException(
                f"Cannot register serializer for '{canonical_type}': "
                f"module '{module_name}' is already imported but does not contain class '{class_name}'"
            )

    # Module not yet imported - set up interceptor
    _ensure_interceptor_installed()
    _serializer_interceptor.register_type_for_module(
        module_name, canonical_type, serializer_config
    )
