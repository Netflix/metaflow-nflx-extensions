import glob
import os
import re
import shutil
import subprocess
import sys

from functools import partial
from itertools import chain
from pathlib import Path

WHITELIST = {
    "README.txt",
    "vendor_any.txt",
    "pip.LICENSE",
}

# Borrowed from https://github.com/pypa/pip/tree/main/src/pip/_vendor

VENDOR_SUBDIR = re.compile(r"^vendor/vendor_([a-zA-Z0-9_]+).txt$")

MF_VENDOR_PREFIX = "metaflow._vendor"
TL_PACKAGE_NAME = "metaflow_extensions"


def delete_all(*paths, whitelist=frozenset()):
    for item in paths:
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        elif item.is_file() and item.name not in whitelist:
            item.unlink()


def find_mf_vendored(*paths, exclude_dirs=frozenset()):
    to_return = []
    for item in paths:
        if item.is_dir() and item.name not in exclude_dirs:
            to_return.append(item.name)
        elif item.is_file() and item.suffix in (".py", ".LICENSE"):
            to_return.append(item.name)
    return to_return


def iter_subtree(path):
    """Recursively yield all files in a subtree, depth-first"""
    if not path.is_dir():
        if path.is_file():
            yield path
        return
    for item in path.iterdir():
        if item.is_dir():
            yield from iter_subtree(item)
        elif item.is_file():
            yield item


def patch_vendor_imports(file, replacements):
    text = file.read_text("utf8")
    for replacement in replacements:
        text = replacement(text)
    file.write_text(text, "utf8")


def find_vendored_libs(vendor_dir, whitelist, whitelist_dirs):
    vendored_libs = []
    paths = []
    for item in vendor_dir.iterdir():
        if item.is_dir() and not item in whitelist_dirs:
            vendored_libs.append(item.name)
        elif item.is_file() and item.name not in whitelist:
            vendored_libs.append(item.stem)  # without extension
        else:  # not a dir or a file not in the whilelist
            continue
        paths.append(item)
    return vendored_libs, paths


def remove_duplicate_vendored(vendor_dir, mf_vendored_names):
    intersec = set((f.name for f in vendor_dir.iterdir())).intersection(
        mf_vendored_names
    )
    delete_all(*[vendor_dir / p for p in intersec])
    return intersec


def fetch_licenses(*info_dir, vendor_dir):
    for file in chain.from_iterable(map(iter_subtree, info_dir)):
        if "LICENSE" in file.name:
            library = file.parent.name.split("-")[0]
            shutil.copy(file, vendor_dir / ("%s.LICENSE" % library))
        else:
            continue


def vendor(vendor_dir, mf_vendor_dir):
    # remove everything
    delete_all(*vendor_dir.iterdir(), whitelist=WHITELIST)

    exclude_subdirs = []
    # Iterate on the vendor*.txt files
    for vendor_file in glob.glob(f"{vendor_dir.name}/vendor*.txt"):
        # We extract the subdirectory we are going to extract into
        subdir = VENDOR_SUBDIR.match(vendor_file).group(1)
        # Includes "any" but it doesn't really matter unless you install "any"
        exclude_subdirs.append(subdir)

    for subdir in exclude_subdirs:
        create_init_file = False
        if subdir == "any":
            vendor_subdir = vendor_dir
            mf_vendor_subdir = mf_vendor_dir
            # target package is <parent>.<vendor_dir>; foo/vendor -> foo.vendor
            pkgname = f"{TL_PACKAGE_NAME}.{vendor_dir.parent.name}.{vendor_dir.name}"
        else:
            create_init_file = True
            vendor_subdir = vendor_dir / subdir
            mf_vendor_subdir = vendor_dir / subdir
            # target package is <parent>.<vendor_dir>; foo/vendor -> foo.vendor
            pkgname = f"{TL_PACKAGE_NAME}.{vendor_dir.parent.name}.{vendor_dir.name}.{vendor_subdir.name}"

        install_args = [
            "python3",
            "-m",
            "pip",
            "install",
            "-t",
            str(vendor_subdir),
            "--no-compile",
            "-r",
            "%s/vendor_%s.txt" % (str(vendor_dir), subdir),
        ]
        # We check in mf_vendor_dir
        if mf_vendor_subdir.is_dir():
            # We actually have stuff here so we figure out what we will need to exclude
            # from what we install
            excluded_installed = find_mf_vendored(
                *mf_vendor_subdir.iterdir(), exclude_dirs=frozenset(exclude_subdirs)
            )
            install_args.extend(
                ["-r", "%s/vendor_%s.txt" % (str(mf_vendor_dir), subdir)]
            )
        else:
            excluded_installed = []
        # install with pip
        subprocess.run(install_args)

        # fetch licenses
        fetch_licenses(*vendor_subdir.glob("*.dist-info"), vendor_dir=vendor_subdir)

        # delete stuff that's not needed
        delete_all(
            *vendor_subdir.glob("*.dist-info"),
            *vendor_subdir.glob("*.egg-info"),
            vendor_subdir / "bin",
        )

        # Touch a __init__.py file
        if create_init_file:
            with open(
                "%s/__init__.py" % str(vendor_subdir), "w+", encoding="utf-8"
            ) as f:
                f.write("# Empty file")

        mf_vendored_libs = remove_duplicate_vendored(vendor_subdir, excluded_installed)
        vendored_libs, paths = find_vendored_libs(
            vendor_subdir, WHITELIST, exclude_subdirs
        )

        replacements = []
        for lib in vendored_libs:
            replacements += (
                partial(  # import bar -> import foo.vendor.bar
                    re.compile(r"(^\s*)import {}\n".format(lib), flags=re.M).sub,
                    r"\1from {} import {}\n".format(pkgname, lib),
                ),
                partial(  # from bar -> from foo.vendor.bar
                    re.compile(r"(^\s*)from {}(\.|\s+)".format(lib), flags=re.M).sub,
                    r"\1from {}.{}\2".format(pkgname, lib),
                ),
            )
        for lib in mf_vendored_libs:
            replacements += (
                partial(  # import bar -> import foo.vendor.bar
                    re.compile(r"(^\s*)import {}\n".format(lib), flags=re.M).sub,
                    r"\1from {} import {}\n".format(
                        ".".join([MF_VENDOR_PREFIX, pkgname]), lib
                    ),
                ),
                partial(  # from bar -> from foo.vendor.bar
                    re.compile(r"(^\s*)from {}(\.|\s+)".format(lib), flags=re.M).sub,
                    r"\1from {}.{}\2".format(
                        ".".join([MF_VENDOR_PREFIX, pkgname]), lib
                    ),
                ),
            )

        for file in chain.from_iterable(map(iter_subtree, paths)):
            if file.suffix == ".py":
                patch_vendor_imports(file, replacements)


if __name__ == "__main__":
    here = Path("__file__").resolve().parent
    vendor_tl_dir = here / "vendor"
    has_vendor_file = len(glob.glob(f"{vendor_tl_dir.name}/vendor*.txt")) > 0
    assert has_vendor_file, "vendor/vendor*.txt file not found"

    # Locate the base vendor directory in the main metaflow distribution.
    assert len(sys.argv) == 2, (
        "Usage %s <path to vendor directory in metaflow>" % sys.argv[0]
    )
    mf_vendor = Path(sys.argv[1]).resolve()
    assert mf_vendor.is_dir(), "%s is not a directory" % str(mf_vendor)

    vendor(vendor_tl_dir, mf_vendor)
