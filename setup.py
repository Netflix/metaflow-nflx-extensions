from setuptools import setup, find_namespace_packages

version = "0.1.0"

setup(
    name="metaflow-netflixext",
    version=version,
    description="EXPERIMENTAL Metaflow extensions from Netflix",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    project_urls={
        "Source": "https://github.com/Netflix/metaflow-nflx-extensions",
        "Tracker": "https://github.com/Netflix/metaflow-nflx-extensions/issues",
    },
    author="Netflix Metaflow Developers",
    author_email="metaflow-dev@netflix.com",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    package_data={
        "metaflow_extensions.netflix_ext.plugins.conda.resources": ["*.png", "*.svg"]
    },
    python_requires=">=3.7.2",
    install_requires=["metaflow>=2.8.3"],
)
