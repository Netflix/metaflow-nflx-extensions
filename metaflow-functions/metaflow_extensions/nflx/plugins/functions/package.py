import hashlib
import json
import os
import site
import time
import zipfile
from io import BytesIO
from typing import Any, Generator, List, Optional, Set, Tuple, cast

# ignore duplicate file warnings
import warnings

warnings.filterwarnings("ignore", module="zipfile", category=UserWarning)
warnings.filterwarnings("ignore", "Duplicate name:", category=UserWarning)


import metaflow
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.package import DEFAULT_SUFFIXES_LIST
from metaflow.util import to_unicode, walk_without_cycles

from metaflow_extensions.nflx.plugins.functions.core.function_spec import (
    FunctionSpec,
)
from metaflow_extensions.nflx.plugins.functions.utils import load_module_from_string


class MetaflowFunctionPackage:
    def __init__(
        self,
        function_spec: FunctionSpec,
        suffixes: Optional[str] = None,
    ) -> None:
        self.suffixes: List[str]
        if suffixes is None:
            self.suffixes = DEFAULT_SUFFIXES_LIST
        else:
            self.suffixes = list(
                set().union(suffixes.split(","), DEFAULT_SUFFIXES_LIST)
            )
        self.metaflow_root: str = os.path.dirname(metaflow.__file__)
        self.name: Optional[str] = function_spec.name
        self.function_spec: FunctionSpec = function_spec
        self.function_roots: Set[str] = self._build_function_roots()
        self.create_time: Optional[float] = None
        self._package_blob: Optional[bytes] = None
        self._package_hash: Optional[str] = None

    def _build_function_roots(self) -> Set[str]:
        """
        Returns the set of directories to be included in the package.
        This includes the directories of the function and its dependencies.

        Returns
        -------
        Set[str]
            A set of directories to be included in the package.
        """
        function_roots = set()
        site_packages: List[str] = site.getsitepackages()

        # Collect all function specs to process
        function_specs = []

        # Add the main function spec
        if self.function_spec.function:
            function_specs.append(self.function_spec.function)

        # Process all function specs in one loop
        for func_spec in function_specs:
            if hasattr(func_spec, "module") and func_spec.module is not None:
                module = load_module_from_string(func_spec.module)
                path = self._get_top_level_module_dir(module)
                # Skip if the module is in site-packages
                if not any(path.startswith(sp) for sp in site_packages):
                    function_roots.add(path)

        return function_roots

    @staticmethod
    def _get_top_level_module_dir(module: Any) -> str:
        """
        Returns the top-level module directory for a given module.

        Parameters
        ----------
        module : Any
            The module for which to get the top-level directory.

        Returns
        -------
        str
            The top-level module directory.
        """
        # Get the full module name, e.g., "foo.bar.baz"
        module_name = module.__name__

        # Extract the top-level module name (e.g., "foo")
        top_level_module_name = module_name.split(".")[0]

        # Import the top-level module
        top_level_module = __import__(top_level_module_name)

        # Check if it's a package or a regular module
        if hasattr(top_level_module, "__path__"):
            # It's a package; __path__[0] is the package directory (e.g., "/path/to/foo")
            path = os.path.dirname(top_level_module.__path__[0])  # Gets "/path/to"
        else:
            # It's a regular module; __file__ is the module file path (e.g., "/path/to/foo.py")
            if not top_level_module.__file__:
                raise ValueError(
                    f"Module {top_level_module_name} has no __file__ attribute"
                )
            path = os.path.dirname(top_level_module.__file__)  # Gets "/path/to"
        return path

    @staticmethod
    def _walk(
        root: str,
        exclude_hidden: bool = True,
        suffixes: Optional[List[str]] = None,
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Walks the directory tree starting from `root` and yields tuples of
        (path, rel_path) for files that match the given suffixes.

        Parameters
        ----------
        root : str
            The root directory to start walking from.
        exclude_hidden : bool, optional
            If True, excludes hidden files and directories.
        suffixes : List[str], optional
            A list of suffixes to match files against. If None, matches all files.

        Yields
        -------
        Tuple[str, str]
            Tuples of (path, rel_path) for files that match the given suffixes.
        """
        if suffixes is None:
            suffixes = []
        root = to_unicode(root)  # handle files/folder with non ascii chars
        for (
            path,
            _,
            files,
        ) in walk_without_cycles(root):
            if exclude_hidden and "/." in path:
                continue
            for fname in files:
                if (fname[0] == "." and fname in suffixes) or (
                    fname[0] != "."
                    and any(fname.endswith(suffix) for suffix in suffixes)
                ):
                    p = os.path.join(path, fname)
                    rel_path = os.path.relpath(path=str(p), start=str(root))
                    yield str(p), rel_path

    def path_tuples(self) -> Generator[Tuple[str, str], None, None]:
        """
        Yields list of (path, arcname) to be added to the job package, where
        `arcname` is the alternative name for the file in the package.

        Yields
        -------
        Tuple[str, str]
            Paths and arcnames to be added to the job package.
        """
        # The user's working directories
        for root in self.function_roots:
            for path, rel_path in self._walk(
                root, exclude_hidden=True, suffixes=self.suffixes
            ):
                arcname = rel_path
                yield path, arcname

    def _create_package(self) -> bytes:
        """
        Creates the ZIP package and computes its hash.

        Returns
        -------
        bytes
            The package blob.
        """
        buf = BytesIO()
        fixed_time = (2019, 12, 3, 0, 0, 0)  # Fixed timestamp: Dec 3, 2019

        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            # Add files from path_tuples
            for path, arcname in self.path_tuples():
                zipinfo = zipfile.ZipInfo(arcname, date_time=fixed_time)
                with open(path, "rb") as f:
                    zipf.writestr(zipinfo, f.read())

            # Add INFO file
            zipinfo = zipfile.ZipInfo("INFO", date_time=fixed_time)
            env = MetaflowEnvironment(None).get_environment_info(include_ext_info=True)
            info_data = json.dumps(env).encode("utf-8")
            zipf.writestr(zipinfo, info_data)

        blob = buf.getvalue()
        self._package_hash = hashlib.sha256(blob).hexdigest()[:32]
        return blob

    def get_package(self) -> bytes:
        """
        Returns the package blob, generating it if necessary.

        Returns
        -------
        bytes
            The package blob.
        """
        if not self._package_blob:
            self.create_time = time.time()
            self._package_blob = self._create_package()
        return self._package_blob

    @property
    def package_hash(self) -> str:
        """
        Returns the hash of the package, generating it if necessary.

        Returns
        -------
        str
            The hash of the package.
        """
        if not self._package_hash:
            _ = self.get_package()  # Trigger package creation
        return cast(str, self._package_hash)

    def __str__(self) -> str:
        return (
            f"<code package for function {self.name} (created @ "
            f"{time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime(self.create_time))})>"
        )
