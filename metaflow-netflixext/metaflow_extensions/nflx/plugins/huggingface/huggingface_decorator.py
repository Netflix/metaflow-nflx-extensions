"""
@huggingface step decorator: auth-managed Hugging Face model access.

Centralizes token/provider resolution, then exposes
current.huggingface.models[key] -> local path. Use models=<list or dict>.
Uses huggingface_hub for metadata and downloads.

By default (``lazy=True``), each snapshot or ``ModelInfo`` is resolved on first access to
the corresponding key. Set ``lazy=False`` to prefetch every listed model in
``task_pre_step`` before the step body runs.
"""

import importlib
import os
import re
import sys
from collections.abc import Mapping as MappingABC
from typing import Dict, Iterator, List, Mapping, Optional, Tuple, Union
from urllib.parse import quote

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_current import current


_BUILTIN_AUTH_PROVIDERS = {
    "env": (
        "metaflow_extensions.nflx.plugins.huggingface.env_auth_provider."
        "EnvHuggingFaceAuthProvider"
    )
}


def _install_current_sentinel() -> None:
    if "huggingface" in current.__class__.__dict__:
        return

    def _raise(ex):
        raise ex

    setattr(
        current.__class__,
        "huggingface",
        property(
            fget=lambda self: _raise(
                RuntimeError(
                    "current.huggingface is only available inside a step "
                    "decorated with @huggingface"
                )
            )
        ),
    )


_install_current_sentinel()


class HuggingFaceContext:
    """
    Context object attached to current.huggingface when @huggingface is used.
    models: key (alias or repo_id) -> local filesystem path (str).
    model_info: when metadata_only=True, key -> ModelInfo from the Hub (no download).

    With lazy resolution (the default), ``models`` / ``model_info`` behave as read-only
    mappings that fetch each key on first access; they are not plain ``dict`` instances.
    """

    def __init__(
        self,
        models: Optional[Union[Dict[str, str], Mapping[str, str]]] = None,
        model_info: Optional[Union[Dict[str, object], Mapping[str, object]]] = None,
    ):
        self.models = models or {}
        self.model_info = model_info or {}


class _LazyRepoMap(MappingABC):
    """Lazily resolves each key in ``spec_map`` to a local path or ``ModelInfo``."""

    def __init__(
        self,
        spec_map: Dict[str, Tuple[str, str]],
        metadata_only: bool,
        token: Optional[str],
        endpoint,
        local_dir_base: str,
    ):
        self._spec_map = spec_map
        self._metadata_only = metadata_only
        self._token = token
        self._endpoint = endpoint
        self._local_dir_base = local_dir_base
        self._cache = {}  # type: Dict[str, Union[str, object]]

    def __getitem__(self, key: str) -> Union[str, object]:
        if key not in self._spec_map:
            raise KeyError(key)
        if key not in self._cache:
            repo_id, revision = self._spec_map[key]
            if self._metadata_only:
                self._cache[key] = _get_model_info(
                    repo_id, revision, self._token, endpoint=self._endpoint
                )
            else:
                self._cache[key] = _download_to_task_dir(
                    repo_id,
                    revision,
                    self._token,
                    self._endpoint,
                    self._local_dir_base,
                )
        return self._cache[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._spec_map)

    def __len__(self) -> int:
        return len(self._spec_map)

    def __contains__(self, key: object) -> bool:
        return key in self._spec_map

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default


def _parse_repo_spec(value: str) -> Tuple[str, str]:
    """Parse 'repo_id' or 'repo_id@revision' into (repo_id, revision)."""
    value = (value or "").strip()
    if not value:
        raise MetaflowException(
            "@huggingface: empty model spec; use repo_id or repo_id@revision"
        )
    if "@" in value:
        repo_id, revision = value.rsplit("@", 1)
        repo_id = repo_id.strip()
        revision = revision.strip()
        if not repo_id or not revision:
            raise MetaflowException(
                "@huggingface: invalid spec '%s'; use repo_id@revision" % value
            )
        return repo_id, revision
    return value, "main"


def _add_specs_from_list(
    spec_map: Dict[str, Tuple[str, str]], entries: List[str]
) -> None:
    for v in entries:
        if not isinstance(v, str):
            raise MetaflowException(
                "@huggingface: models list must contain strings, got %s" % type(v)
            )
        repo_id, revision = _parse_repo_spec(v)
        if repo_id in spec_map:
            raise MetaflowException(
                "@huggingface: duplicate model repo '%s' in models list. "
                "Use a models dict with aliases to reference multiple revisions."
                % repo_id
            )
        spec_map[repo_id] = (repo_id, revision)


def _add_specs_from_dict(
    spec_map: Dict[str, Tuple[str, str]], mapping: Dict[str, str]
) -> None:
    for k, v in mapping.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise MetaflowException("@huggingface: models dict must be str -> str")
        if not k.strip():
            raise MetaflowException("@huggingface: models dict aliases cannot be empty")
        repo_id, revision = _parse_repo_spec(v)
        spec_map[k] = (repo_id, revision)


def _build_spec_map(
    models: Optional[Union[List[str], Dict[str, str]]],
) -> Dict[str, Tuple[str, str]]:
    """Build key -> (repo_id, revision). models is a list or alias dict."""
    spec_map: Dict[str, Tuple[str, str]] = {}
    if not models:
        return spec_map
    if isinstance(models, list):
        _add_specs_from_list(spec_map, models)
    elif isinstance(models, dict):
        _add_specs_from_dict(spec_map, models)
    else:
        raise MetaflowException(
            "@huggingface: models must be a list or dict, got %s" % type(models)
        )
    return spec_map


def _import_provider_class(provider_type: str, import_path: str):
    if not isinstance(import_path, str):
        raise MetaflowException(
            "@huggingface: auth provider '%s' import path must be a string"
            % provider_type
        )
    module_path, _, class_name = import_path.rpartition(".")
    if not module_path or not class_name:
        raise MetaflowException(
            "@huggingface: auth provider '%s' has invalid import path '%s'"
            % (provider_type, import_path)
        )
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        raise MetaflowException(
            "@huggingface: failed to import auth provider '%s' from '%s': %s"
            % (provider_type, import_path, e)
        ) from e
    try:
        provider_cls = getattr(module, class_name)
    except AttributeError as e:
        raise MetaflowException(
            "@huggingface: auth provider '%s' class '%s' not found in '%s'"
            % (provider_type, class_name, module_path)
        ) from e
    declared_type = getattr(provider_cls, "TYPE", None)
    if declared_type != provider_type:
        raise MetaflowException(
            "@huggingface: auth provider '%s' at '%s' declares TYPE '%s'"
            % (provider_type, import_path, declared_type)
        )
    return provider_cls


def _get_auth_provider():
    from metaflow.metaflow_config import (
        HUGGINGFACE_AUTH_PROVIDER,
        HUGGINGFACE_AUTH_PROVIDERS,
    )

    provider_type = HUGGINGFACE_AUTH_PROVIDER or "env"
    if not isinstance(HUGGINGFACE_AUTH_PROVIDERS, dict):
        raise MetaflowException(
            "@huggingface: HUGGINGFACE_AUTH_PROVIDERS must be a dict mapping "
            "provider name to import path"
        )
    providers = dict(_BUILTIN_AUTH_PROVIDERS)
    providers.update(HUGGINGFACE_AUTH_PROVIDERS)
    import_path = providers.get(provider_type)
    if import_path is None:
        raise MetaflowException(
            "@huggingface: unknown auth provider '%s'. Configure "
            "HUGGINGFACE_AUTH_PROVIDERS with a provider import path or use 'env'."
            % provider_type
        )
    return _import_provider_class(provider_type, import_path)()


def _import_snapshot_download():
    try:
        from huggingface_hub import snapshot_download
    except ImportError as e:
        raise MetaflowException(
            "@huggingface requires the 'huggingface_hub' package. "
            "Install it with: pip install huggingface_hub or "
            "pip install metaflow-netflixext[huggingface]. Error: %s" % e
        ) from e
    return snapshot_download


def _import_hf_api():
    try:
        from huggingface_hub import HfApi
    except ImportError as e:
        raise MetaflowException(
            "@huggingface requires the 'huggingface_hub' package. "
            "Install it with: pip install huggingface_hub or "
            "pip install metaflow-netflixext[huggingface]. Error: %s" % e
        ) from e
    return HfApi


def _download_model(
    repo_id: str,
    revision: str,
    token: Optional[str],
    local_dir: str,
    endpoint: Optional[str] = None,
) -> str:
    snapshot_download = _import_snapshot_download()
    kwargs = dict(
        repo_id=repo_id,
        revision=revision,
        token=token,
        local_dir=local_dir,
    )
    if endpoint is not None:
        kwargs["endpoint"] = endpoint
    return snapshot_download(**kwargs)


def _model_info_404_hint(repo_id: str) -> str:
    return (
        "Token was obtained but the Hub returned 404. "
        "Ensure the token has read access to repo '%s' and is for the correct account "
        "(e.g. enterprise vs open-source)."
    ) % repo_id


def _get_model_info(
    repo_id: str,
    revision: str,
    token: Optional[str],
    endpoint: Optional[str] = None,
) -> object:
    """Fetch model metadata from the Hub without downloading files."""
    HfApi = _import_hf_api()
    base_url = endpoint or "https://huggingface.co"
    api = HfApi(token=token, endpoint=base_url)
    try:
        return api.model_info(repo_id, revision=revision, token=token)
    except Exception as e:
        err_str = str(e).lower()
        if token and (
            "404" in err_str or "not found" in err_str or "repository" in err_str
        ):
            raise MetaflowException(
                "@huggingface: failed to get model info for %s@%s from %s: %s. %s"
                % (repo_id, revision, base_url, e, _model_info_404_hint(repo_id))
            ) from e
        raise


def _log_auth_provider(provider_type: str, token: Optional[str]) -> None:
    msg = "@huggingface: using auth provider '%s', token %s"
    sys.stderr.write(msg % (provider_type, "obtained" if token else "none") + "\n")


def _resolve_auth_token():
    """Return the token from the configured auth provider, or None."""
    auth_provider = _get_auth_provider()
    provider_type = getattr(auth_provider, "TYPE", "unknown")
    token = auth_provider.get_token()
    _log_auth_provider(provider_type, token)
    return token


def _resolve_local_dir_base(decorator_local_dir: Optional[str]) -> str:
    """
    Parent directory for model snapshots: ``<base>/<repo_sanitized>_<revision>/``,
    with ``/`` in ``repo_id`` replaced by ``--`` and revision values encoded as
    a single filesystem component.

    Order: non-empty ``@huggingface(local_dir=...)``, then
    ``METAFLOW_HUGGINGFACE_LOCAL_DIR`` / ``HUGGINGFACE_LOCAL_DIR``, else
    ``<task temp>/metaflow_huggingface``.
    """
    if isinstance(decorator_local_dir, str):
        s = decorator_local_dir.strip()
        if s:
            return os.path.abspath(os.path.expanduser(s))
    from metaflow import metaflow_config

    config_local_dir = getattr(metaflow_config, "HUGGINGFACE_LOCAL_DIR", None)
    if config_local_dir:
        return os.path.abspath(os.path.expanduser(str(config_local_dir).strip()))
    return os.path.join(current.tempdir or "/tmp", "metaflow_huggingface")


def _safe_path_component(value: str, label: str, encode: bool = False) -> str:
    value = (value or "").strip()
    if not value:
        raise MetaflowException("@huggingface: empty %s is not valid" % label)
    if encode:
        component = quote(value, safe="._-")
    else:
        component = value.replace("/", "--").replace("\\", "--")
        component = re.sub(r"[^A-Za-z0-9._-]+", "--", component)
    if component in ("", ".", ".."):
        raise MetaflowException(
            "@huggingface: %s '%s' cannot be used as a download path component"
            % (label, value)
        )
    return component


def _download_to_task_dir(
    repo_id: str,
    revision: str,
    token: Optional[str],
    endpoint,
    local_dir_base: str,
) -> str:
    local_dir_base = os.path.abspath(os.path.expanduser(local_dir_base))
    repo_component = _safe_path_component(repo_id, "repo id")
    revision_component = _safe_path_component(revision, "revision", encode=True)
    task_subdir = os.path.abspath(
        os.path.join(local_dir_base, "%s_%s" % (repo_component, revision_component))
    )
    if os.path.commonpath([local_dir_base, task_subdir]) != local_dir_base:
        raise MetaflowException(
            "@huggingface: resolved download path escaped local_dir base"
        )
    os.makedirs(local_dir_base, exist_ok=True)
    return _download_model(repo_id, revision, token, task_subdir, endpoint=endpoint)


def _fill_huggingface_maps(
    spec_map: Dict[str, Tuple[str, str]],
    metadata_only: bool,
    token: Optional[str],
    endpoint,
    local_dir_base: str,
) -> Tuple[Dict[str, str], Dict[str, object]]:
    path_map = {}
    info_map = {}
    for key, (repo_id, revision) in spec_map.items():
        if metadata_only:
            info_map[key] = _get_model_info(repo_id, revision, token, endpoint=endpoint)
        else:
            path_map[key] = _download_to_task_dir(
                repo_id, revision, token, endpoint, local_dir_base
            )
    return path_map, info_map


class HuggingFaceDecorator(StepDecorator):
    """
    Declares HuggingFace models needed for this step. Auth is resolved through the
    configured provider; model paths are exposed via current.huggingface.models[key].
    By default uses https://huggingface.co. Set METAFLOW_HUGGINGFACE_ENDPOINT only if
    your Hub is hosted at a different URL.

    Parameters
    ----------
    models : list or dict, optional
        Either a list of repo specs, e.g.
        ["meta-llama/Llama-2-7b", "bert-base-uncased@v1.0"],
        or a dict of alias -> repo spec, e.g.
        {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"}.
    metadata_only : bool, optional
        If True, only fetch model metadata from the Hub (no file download).
    lazy : bool, optional
        If True (default), resolve auth in ``task_pre_step`` but fetch each snapshot or
        ``ModelInfo`` on first access.
    local_dir : str, optional
        Absolute or relative path to the parent directory for downloaded snapshots.

    MF Add To Current
    -----------------
    huggingface -> HuggingFaceContext
        Object with ``models`` (key -> local path when not metadata_only) and
        ``model_info`` (key -> ModelInfo when metadata_only=True).
    """

    name = "huggingface"
    defaults = {"models": None, "metadata_only": False, "lazy": True, "local_dir": None}

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        models = self.attributes.get("models")
        self._metadata_only = self.attributes.get("metadata_only", False)
        self._lazy = self.attributes.get("lazy", True)
        if not isinstance(self._metadata_only, bool):
            raise MetaflowException(
                "@huggingface: metadata_only must be a boolean, got %s"
                % type(self._metadata_only).__name__
            )
        if not isinstance(self._lazy, bool):
            raise MetaflowException(
                "@huggingface: lazy must be a boolean, got %s"
                % type(self._lazy).__name__
            )
        local_dir = self.attributes.get("local_dir")
        if local_dir is not None and not isinstance(local_dir, str):
            raise MetaflowException(
                "@huggingface: local_dir must be a string filesystem path or None"
            )
        self._decorator_local_dir = local_dir
        if not models:
            raise MetaflowException("@huggingface: specify 'models' (list or dict)")
        self._spec_map = _build_spec_map(models)
        if not self._spec_map:
            raise MetaflowException(
                "@huggingface: 'models' must contain at least one entry"
            )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        try:
            token = _resolve_auth_token()
        except Exception as e:
            raise MetaflowException("@huggingface: auth provider failed: %s" % e) from e

        from metaflow.metaflow_config import HUGGINGFACE_ENDPOINT

        endpoint = HUGGINGFACE_ENDPOINT
        local_dir_base = _resolve_local_dir_base(self._decorator_local_dir)
        if self._lazy:
            if self._metadata_only:
                ctx = HuggingFaceContext(
                    models={},
                    model_info=_LazyRepoMap(
                        self._spec_map, True, token, endpoint, local_dir_base
                    ),
                )
            else:
                ctx = HuggingFaceContext(
                    models=_LazyRepoMap(
                        self._spec_map, False, token, endpoint, local_dir_base
                    ),
                    model_info={},
                )
        else:
            path_map, info_map = _fill_huggingface_maps(
                self._spec_map, self._metadata_only, token, endpoint, local_dir_base
            )
            ctx = HuggingFaceContext(models=path_map, model_info=info_map)
        current._update_env({"huggingface": ctx})
