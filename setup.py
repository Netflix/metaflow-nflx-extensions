from setuptools import setup, find_namespace_packages

version = "0.0.1"

setup(
    name="metaflow-netflixext",
    version=version,
    description="EXPERIMENTAL Metaflow extensions from Netflix",
    author="Netflix Metaflow Developers",
    author_email="metaflow-dev@netflix.com",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    install_requires=["metaflow>=2.7.16"],
)
