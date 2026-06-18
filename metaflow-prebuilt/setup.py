from setuptools import setup, find_namespace_packages

with open("VERSION", mode="r") as f:
    version = f.read().strip()

setup(
    name="metaflow-prebuilt",
    version=version,
    description=(
        "Metaflow extension: pre-bake conda environments into Docker images "
        "for fast cold starts"
    ),
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
    # Mirrors the internal minimum (support for <3.10 was dropped internally). The
    # code has no 3.10-only construct — _parse_build_system_requires uses a
    # tomllib/tomli/regex fallback chain that works on any version.
    python_requires=">=3.10",
    install_requires=["metaflow>=2.16.0", "metaflow-netflixext>=1.3.12"],
    extras_require={
        "ecr": ["boto3"],
        "codebuild": ["boto3"],
        "gcr": ["google-cloud-storage"],
        "kaniko": ["google-cloud-storage", "kubernetes"],
        "dev": ["pytest>=7", "pytest-mock", "tox>=4"],
    },
    entry_points={
        "metaflow_prebuilt.build_services": [
            "docker = metaflow_extensions.prebuilt.plugins.conda.services.docker_service:LocalDockerBuildService",
            "kaniko = metaflow_extensions.prebuilt.plugins.conda.services.kaniko_service:KanikoBuildService",
            "buildx = metaflow_extensions.prebuilt.plugins.conda.services.buildx_service:BuildxBuildService",
            "codebuild = metaflow_extensions.prebuilt.plugins.conda.services.codebuild_service:CodeBuildService",
        ],
        "metaflow_prebuilt.image_registries": [
            "ecr = metaflow_extensions.prebuilt.plugins.conda.registries.ecr_registry:ECRRegistry",
            "gcr = metaflow_extensions.prebuilt.plugins.conda.registries.gcr_registry:GCRRegistry",
            "dockerhub = metaflow_extensions.prebuilt.plugins.conda.registries.dockerhub_registry:DockerHubRegistry",
            "local = metaflow_extensions.prebuilt.plugins.conda.registries.local_registry:LocalRegistry",
        ],
    },
)
