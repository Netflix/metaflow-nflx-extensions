# pyright: strict, reportTypeCommentUsage=false, reportMissingTypeStubs=false
from typing import Dict, List, Optional, Tuple

from ..env_descr import (
    EnvType,
    ResolvedEnvironment,
)

from . import Resolver


class CondaLockResolver(Resolver):
    TYPES = ["micromamba_server"]

    def resolve(
        self,
        env_type: EnvType,
        deps: Dict[str, List[str]],
        sources: Dict[str, List[str]],
        extras: Dict[str, List[str]],
        architecture: str,
        builder_envs: Optional[List[ResolvedEnvironment]] = None,
        base_env: Optional[ResolvedEnvironment] = None,
    ) -> Tuple[ResolvedEnvironment, Optional[List[ResolvedEnvironment]]]:
        raise NotImplementedError

    # The below code is not maintained and kept here as a reference -- it probably
    # needs to be cleaned up
    # deps = [d for d in deps if d.category in ("conda", "npconda")]

    # if not self._have_micromamba_server:
    #     raise CondaException(
    #         "Micromamba server not supported by installed version of micromamba"
    #     )

    # self._start_micromamba_server()
    # # Form the payload to send to the server
    # req = {
    #     "specs": [d.value for d in deps if d.category in ("conda", "npconda")]
    #     + ["pip"],
    #     "platform": architecture,
    #     "channels": [c.value for c in channels if c.category == "conda"],
    # }
    # if arch_id() == architecture:
    #     # Use the same virtual packages as the ones used for conda/mamba
    #     req["virtual_packages"] = [
    #         "%s=%s" % (virt_pkg, virt_build_str)
    #         for virt_pkg, virt_build_str in self.virtual_packages.items()
    #     ]
    # # Make the request to the micromamba server
    # debug.conda_exec(
    #     "Payload for micromamba server on port %d: %s"
    #     % (self._micromamba_server_port, str(req))
    # )
    # resp = requests.post(
    #     "http://localhost:%d" % self._micromamba_server_port, json=req
    # )
    # if resp.status_code != 200:
    #     raise CondaException(
    #         "Got unexpected return code from micromamba server: %d"
    #         % resp.status_code
    #     )
    # else:
    #     json_response = resp.json()
    #     if "error_msg" in json_response:
    #         raise CondaException(
    #             "Cannot resolve environment: %s" % json_response["error_msg"]
    #         )
    #     else:
    #         return [
    #             CondaPackageSpecification(
    #                 filename=pkg["filename"],
    #                 url=pkg["url"],
    #                 url_format=correct_splitext(pkg["filename"])[1],
    #                 hashes={correct_splitext(pkg["filename"])[1]: pkg["md5"]},
    #             )
    #             for pkg in json_response["packages"]
    #         ]
