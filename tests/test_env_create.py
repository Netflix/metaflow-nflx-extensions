import os
import sys
import uuid
import pytest
import shutil

import sh


my_dir = os.path.dirname(os.path.abspath(__file__))

all_tests = []

trans_table = str.maketrans(".-", "__")

try:
    os.symlink(os.path.join(my_dir, "foo_pkg"), "/tmp/build_foo_pkg")
except FileExistsError:
    pass

with os.scandir(os.path.join(my_dir, "environments")) as it:
    for entry in it:
        if entry.is_file() and (
            entry.name.endswith(".yml") or entry.name.endswith(".txt")
        ):
            if entry.name.endswith(".yml"):
                flag = "-f"
            else:
                flag = "-r"
            if entry.name.startswith("no_python_"):
                python_versions = ["file"]
            elif entry.name.startswith("pip-version"):
                python_versions = ["3.8.*", "3.10.*"]
            else:
                python_versions = ["3.8.*"]
            for pv in python_versions:
                all_tests.extend(
                    [
                        (pv, flag, entry.name, None),
                        (
                            pv,
                            flag,
                            entry.name,
                            "metaflow/tests/env_resolution/%s"
                            % entry.name.translate(trans_table),
                        ),
                    ]
                )


@pytest.mark.parametrize(
    argnames=["python_version", "file_type", "file_name", "alias"], argvalues=all_tests
)
def test_resolve_and_check_env(capsys, python_version, file_type, file_name, alias):
    cwd = os.getcwd()
    os.chdir(my_dir)
    try:
        conda_rand = str(uuid.uuid4())

        env_dict = dict(os.environ)
        env_dict["METAFLOW_CONDA_ENVS_DIRNAME"] = "testing/envs_%s" % conda_rand
        env_dict["METAFLOW_CONDA_PACKAGES_DIRNAME"] = "testing/packages_%s" % conda_rand
        env_dict["METAFLOW_CONDA_MAGIC_FILE_V2"] = "condav2-%s.cnd" % conda_rand
        env_dict["METAFLOW_CONDA_LOCK_TIMEOUT"] = (
            "7200"  # Increase to make sure we resolve everything
        )
        env_dict["METAFLOW_DEBUG_CONDA"] = "1"
        env_dict["CONDA_ENVS_DIRS"] = "/tmp/mfcondaenvs-%s" % conda_rand
        env_dict["CONDA_PKGS_DIRS"] = "/tmp/mfcondapkgs-%s" % conda_rand
        check_command = sh.Command("./check_env.sh").bake(["-e", sys.executable])
        metaflow_command = sh.Command(sys.executable).bake(
            ["-m", "metaflow.cmd.main_cli"]
        )
        if alias:
            check_command(
                "-p",
                python_version,
                file_type,
                file_name,
                "-a",
                alias,
                _env=env_dict,
                _truncate_exc=False,
            )
            # We try using the environment by adding a tiny package.
            if file_name.endswith(".yml"):
                other_file = "./environments/itsdangerous.yml"
            else:
                other_file = "./environments/itsdangerous.txt"
            if file_name.startswith("itsdangerous"):
                with pytest.raises(sh.ErrorReturnCode):
                    # This should fail because the environment already exists.
                    metaflow_command(
                        "environment",
                        "resolve",
                        "--using",
                        alias,
                        file_type,
                        other_file,
                        _env=env_dict,
                        _truncate_exc=False,
                    )
            else:
                metaflow_command(
                    "environment",
                    "resolve",
                    "--using",
                    alias,
                    file_type,
                    other_file,
                    _env=env_dict,
                    _truncate_exc=False,
                )
        else:
            check_command(
                "-p",
                python_version,
                file_type,
                file_name,
                _env=env_dict,
                _truncate_exc=False,
            )
    finally:
        os.chdir(cwd)
        # Runners run out of space so clear out all the packages and environments we created/downloaded
        shutil.rmtree(
            os.path.join(
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"],
                "conda_env",
                "testing",
                "envs_%s" % conda_rand,
            ),
            ignore_errors=True,
        )
        shutil.rmtree(
            os.path.join(
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"],
                "conda_env",
                "testing",
                "packages_%s" % conda_rand,
            ),
            ignore_errors=True,
        )
        shutil.rmtree(env_dict["CONDA_ENVS_DIRS"], ignore_errors=True)
        shutil.rmtree(env_dict["CONDA_PKGS_DIRS"], ignore_errors=True)
