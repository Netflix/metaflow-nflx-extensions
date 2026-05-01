import os
from setuptools import setup, find_namespace_packages

with open("VERSION", mode="r") as f:
    version = f.read().strip()

# We write a version file directly in the code so it can be read in the python code
with open(
    os.path.join(
        os.path.dirname(__file__),
        "metaflow_extensions",
        "nflx",
        "toplevel",
        "nflxext_version.py",
    ),
    "wt",
) as f:
    f.write('nflxext_version = "%s"\n' % version)

setup(
    name="metaflow-netflixext",
    version=version,
    description="Metaflow extensions from Netflix",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    project_urls={
        "Source": "https://github.com/Netflix/metaflow-nflx-extensions",
        "Tracker": "https://github.com/Netflix/metaflow-nflx-extensions/issues",
    },
    author="Netflix Metaflow Developers",
    author_email="metaflow-dev@netflix.com",
    packages=find_namespace_packages(
        include=["metaflow_extensions", "metaflow_extensions.*"]
    ),
    py_modules=[
        "metaflow_extensions",
    ],
    package_data={
        "metaflow_extensions.nflx.plugins.conda.resources": ["*.png", "*.svg"]
    },
    python_requires=">=3.7.2",
    install_requires=["metaflow>=2.16.0"],
    extras_require={
        "huggingface": ["huggingface_hub"],
    },
)
