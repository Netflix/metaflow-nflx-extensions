"""
Utilities for syncing Metaflow serializers with Ray's serialization system.
"""

from metaflow_extensions.nflx.plugins.functions.debug import debug


def sync_ray_serializers():
    """
    Sync Metaflow serializers to Ray's serialization system.

    Reads the Metaflow global serializer registry and registers all serializers
    with Ray using their dumps/loads methods.

    This keeps both serialization systems in sync:
    - Serialization logic is defined once in the Metaflow registry
    - Ray uses the same dumps/loads interface
    - No additional serialization overhead

    The function is idempotent and safe to call multiple times.
    """
    import ray

    from metaflow_extensions.nflx.plugins.functions.serializers.registry import (
        get_global_registry,
    )

    registry = get_global_registry()

    # Get all registered types from the Metaflow registry
    registered_types = registry.get_registered_types()

    debug.functions_exec(
        f"sync_ray_serializers: Found {len(registered_types)} registered types in Metaflow registry"
    )

    for canonical_type in registered_types:
        try:
            # Lazy-load the serializer instance for this type
            serializer = registry._load_serializer(canonical_type)

            # Load the actual Python type class from the canonical string
            from metaflow_extensions.nflx.plugins.functions.serializers.config import (
                load_type_from_canonical_string,
            )

            type_cls = load_type_from_canonical_string(canonical_type)
            if type_cls is None:
                debug.functions_exec(
                    f"sync_ray_serializers: Could not load type class for {canonical_type}"
                )
                continue

            # Register this type with Ray using dumps/loads methods
            ray.util.register_serializer(
                type_cls,
                serializer=serializer.dumps,
                deserializer=serializer.loads,
            )

            debug.functions_exec(
                f"sync_ray_serializers: Registered {canonical_type} with Ray"
            )

        except Exception as e:
            # Continue syncing other types even if one fails
            # (Partial sync is better than complete failure)
            debug.functions_exec(
                f"sync_ray_serializers: Error registering {canonical_type}: {e}"
            )
            continue

    debug.functions_exec("sync_ray_serializers: Completed")
