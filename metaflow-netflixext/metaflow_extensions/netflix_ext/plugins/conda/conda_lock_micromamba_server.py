glue_script = """#! {python_executable}

import json
import os
import subprocess
import sys

import requests


def parse_args(args):
    # Poor man's parsing of arguments; we just need to extract channels and
    # whatever is at the end for the specs. We can ignore everything else
    channels = []
    virtual_package_channels = None
    specs = []
    next_is_channel = False
    after_channels = False
    for arg in args:
        if next_is_channel:
            channels.append(arg)
            virtual_package_channels = arg  # Virtual package one is always last
            next_is_channel = False
        elif arg == "--channel":
            next_is_channel = True
            after_channels = True
        elif after_channels:
            specs.append("==".join(arg.split(" ")))
    return channels, specs, virtual_package_channels


def extract_virtual_packages(path):
    virtual_packages = []
    with open(path, mode="r", encoding="utf-8") as f:
        virtual_package_descr = json.load(f)
    for pkg in virtual_package_descr["packages"].values():
        virtual_packages.append("=".join([pkg["name"], *pkg["version"].split("-")]))
    return virtual_packages


if __name__ == "__main__":
    platform = os.environ.get("CONDA_SUBDIR")
    if len(sys.argv) < 2 or sys.argv[1] != "create":
        # Pass to micromamba directly
        exit(
            subprocess.call(
                ["{micromamba_exec}", "-r", "{micromamba_root}"] + sys.argv[1:],
                shell=True
        ))
        
    channels, specs, virtual_package_channels = parse_args(sys.argv[1:])
    virtual_packages = extract_virtual_packages(
        os.path.join(virtual_package_channels[7:], platform, "repodata.json")
    )
    resp = requests.post(
        "http://localhost:{server_port}/solve",
        json= {{
            "specs": specs,
            "channels": channels,
            "platform": platform,
            "virtual_packages": virtual_packages,
        }},
    )
    if resp.status_code != 200:
        print(
            json.dumps(
                {{
                    "success": False,
                    "solver_problems": "Got %d status from micromamba server"
                    % resp.status_code,
                }}
            )
        )
        exit(1)
    json_response = resp.json()
    if "error_msg" in json_response:
        print(
            json.dumps(
                {{"success": False, "solver_problems": json_response["error_msg"]}}
            )
        )
        exit(1)
    # Here we create a response that is similar to what would be created by pure
    # micromamba but only need the FETCH portion as that is the only thing conda-lock
    # cares about in the end.
    print(
        json.dumps(
            {{
                "success": True,
                "dry_run": True,
                "prefix": "/dev/null",
                "actions": {{
                    "FETCH": json_response["packages"],
                    "LINK": [],
                    "PREFIX": "/dev/null",
                }},
            }}
        )
    )
"""
