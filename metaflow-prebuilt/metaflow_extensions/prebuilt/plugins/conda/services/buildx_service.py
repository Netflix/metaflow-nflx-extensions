import os
import shutil
import subprocess
import tempfile
from typing import Any, Callable, Dict, Optional

from ..build_service import DockerBuildService


class BuildxBuildService(DockerBuildService):
    """Build service using Docker Buildx (``docker buildx build --push``).

    Supports remote builders and cross-platform builds. Requires Docker
    with the buildx plugin.

    Configure with:
        METAFLOW_PREBUILT_BUILDX_BUILDER — builder name (default: current builder)
    """

    def build_and_push(
        self,
        dockerfile: str,
        context_files: Dict[str, Any],
        image_tag: str,
        push_credentials: Dict[str, Any],
        echo: Callable[..., None],
        target_platform: Optional[str] = None,
    ) -> bool:
        build_dir = tempfile.mkdtemp(prefix="metaflow_prebuilt_buildx_")
        try:
            with open(os.path.join(build_dir, "Dockerfile"), "w") as f:
                f.write(dockerfile)

            for fname, content in context_files.items():
                full_path = os.path.join(build_dir, fname)
                os.makedirs(os.path.dirname(full_path) or build_dir, exist_ok=True)
                if isinstance(content, (bytes, bytearray)):
                    with open(full_path, "wb") as f:
                        f.write(content)
                else:
                    with open(full_path, "w") as f:
                        f.write(content)

            cmd = ["docker", "buildx", "build", "--push", "--tag", image_tag]

            # Build for the REMOTE step's arch, not the builder's default. Without
            # this, an arm64 laptop deploying a linux-64 step would bake an arm64
            # image whose in-container installer arch_id() mismatches the resolved
            # manifest. None => builder default (same-arch deploy, unchanged).
            if target_platform:
                cmd += ["--platform", target_platform]

            builder = os.environ.get("METAFLOW_PREBUILT_BUILDX_BUILDER", "")
            if builder:
                cmd += ["--builder", builder]

            cmd.append(".")
            echo(
                "    docker buildx build --push%s -t %s ..."
                % (
                    " --platform %s" % target_platform if target_platform else "",
                    image_tag,
                )
            )

            try:
                result = subprocess.run(
                    cmd,
                    cwd=build_dir,
                    capture_output=True,
                    text=True,
                    timeout=1800,
                )
            except FileNotFoundError:
                echo("    ERROR: docker not found on PATH. Install Docker with buildx.")
                return False
            except subprocess.TimeoutExpired as e:
                echo("    ERROR: buildx timed out: %s" % e)
                return False

            if result.returncode != 0:
                echo(
                    "    ERROR: buildx failed (exit %d):\n%s\n%s"
                    % (
                        result.returncode,
                        result.stdout.rstrip()[-2000:],
                        result.stderr.rstrip()[-2000:],
                    )
                )
                return False

            return True
        finally:
            shutil.rmtree(build_dir, ignore_errors=True)
