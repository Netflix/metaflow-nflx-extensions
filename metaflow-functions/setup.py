from setuptools import setup, find_namespace_packages

with open("VERSION", mode="r") as f:
    version = f.read().strip()

setup(
    name="metaflow-functions",
    version=version,
    description="Metaflow Functions",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
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
    python_requires=">=3.10",
    install_requires=[
        "metaflow>=2.16.0",
        "psutil>=5.8.0",
        "cffi",
        "fastavro",
        "ray",
    ],
)
