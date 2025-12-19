# File that is used when running pip in cross-platform resolution.
import json
import os
import pip._vendor.packaging.markers as _markers


_overrides = os.environ.get("PIP_CUSTOMIZE_OVERRIDES")
if _overrides:
    _orig_default_environment = _markers.default_environment
    _overrides_dict = json.loads(_overrides)

    def _wrap_default_environment():
        result = _orig_default_environment()
        result.update(_overrides_dict)
        return result

    _markers.default_environment = _wrap_default_environment
