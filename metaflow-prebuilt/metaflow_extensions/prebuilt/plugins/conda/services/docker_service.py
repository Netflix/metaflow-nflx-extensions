import os
import shutil
import subprocess
import tempfile
from typing import Any, Callable, Dict, Optional

from ..build_service import DockerBuildService


class LocalDockerBuildService(DockerBuildService):
    """Build service that uses the local Docker daemon.

    Requires Docker installed and authenticated to the target registry on
    the deploy machine. Runs ``docker build -t <tag> <dir> && docker push <tag>``.

    ``push_credentials`` from the registry is ignored — authentication is
    expected to be handled by ``docker login`` before deploying.
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
        build_dir = tempfile.mkdtemp(prefix="metaflow_prebuilt_docker_")
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

            # --platform builds for the REMOTE step's arch (not the daemon's
            # default), so a cross-arch deploy bakes an image matching the resolved
            # manifest. None => daemon default (same-arch deploy, unchanged).
            build_cmd = ["docker", "build", "-t", image_tag]
            if target_platform:
                build_cmd += ["--platform", target_platform]
            build_cmd.append(".")
            echo(
                "    docker build%s -t %s ..."
                % (
                    " --platform %s" % target_platform if target_platform else "",
                    image_tag,
                )
            )
            build_result = subprocess.run(
                build_cmd,
                cwd=build_dir,
                capture_output=True,
                text=True,
                timeout=1800,
            )
            if build_result.returncode != 0:
                echo(
                    "    ERROR: docker build failed (exit %d):\n%s\n%s"
                    % (
                        build_result.returncode,
                        build_result.stdout.rstrip()[-2000:],
                        build_result.stderr.rstrip()[-2000:],
                    )
                )
                return False

            echo("    docker push %s ..." % image_tag)
            push_result = subprocess.run(
                ["docker", "push", image_tag],
                capture_output=True,
                text=True,
                timeout=600,
            )
            if push_result.returncode != 0:
                echo(
                    "    ERROR: docker push failed (exit %d):\n%s\n%s"
                    % (
                        push_result.returncode,
                        push_result.stdout.rstrip()[-2000:],
                        push_result.stderr.rstrip()[-2000:],
                    )
                )
                return False

            return True
        except subprocess.TimeoutExpired as e:
            echo("    ERROR: docker command timed out: %s" % e)
            return False
        except FileNotFoundError:
            echo("    ERROR: docker not found on PATH. Install Docker and retry.")
            return False
        finally:
            shutil.rmtree(build_dir, ignore_errors=True)
