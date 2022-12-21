from setuptools import setup, find_namespace_packages

version = "0.0.1"

setup(
    name="metaflow-netflixext",
    version=version,
    description="EXPERIMENTAL Metaflow extensions from Netflix",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Netflix Metaflow Developers",
    author_email="metaflow-dev@netflix.com",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    python_requires='>3.5',
    install_requires=["metaflow>=2.7.16"],
)
