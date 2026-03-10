import os
import platform

from setuptools import find_namespace_packages, setup
from setuptools.command.build_py import build_py

cur_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(cur_dir, "VERSION"), "rt") as f:
    version = f.read().strip()

with open(os.path.join(cur_dir, "requirements_non_mf.txt"), "rt") as f:
    requirements = f.read().splitlines()

requirements.append("metaflow>=2.16.0")

# Write version file so it can be imported
with open(
    os.path.join(
        cur_dir,
        "metaflow_extensions",
        "fastdata_ext",
        "toplevel",
        "fastdata_version.py",
    ),
    "wt",
) as f:
    f.write('_ext_version = "%s"\n' % version)


class BuildDataLib(build_py):
    """Custom command to build the metaflow-data.so C++ library"""

    def run(self):
        if platform.system() in ["Linux", "Darwin"]:
            import subprocess

            orig_dir = os.getcwd()
            src_dir = os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                "metaflow_extensions",
                "fastdata_ext",
                "plugins",
                "datatools",
                "cpp",
            )
            if os.path.isdir(src_dir):
                try:
                    os.chdir(src_dir)
                    subprocess.check_call(["make", "release"])
                    subprocess.check_call(["make", "copy"])
                finally:
                    os.chdir(orig_dir)

        build_py.run(self)


setup(
    name="metaflow-fastdata",
    version=version,
    description="Metaflow FastData - OSS high-performance data access for Hive/Iceberg tables",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    project_urls={
        "Source": "https://github.com/Netflix/metaflow-nflx-extensions",
        "Tracker": "https://github.com/Netflix/metaflow-nflx-extensions/issues",
    },
    author="Netflix Metaflow Developers",
    author_email="metaflow-dev@netflix.com",
    packages=find_namespace_packages(
        include=["metaflow_extensions", "metaflow_extensions.*"],
        exclude=["metaflow_extensions.fastdata_ext.plugins.datatools.cpp*"],
    ),
    py_modules=[
        "metaflow_extensions",
    ],
    cmdclass={"build_py": BuildDataLib},
    package_data={
        "metaflow_extensions.fastdata_ext.plugins.datatools": [
            "bin/*.so",
            "bin/c_api_funcs.h",
            "*.json",
        ],
    },
    install_requires=requirements,
    python_requires=">=3.8",
)
