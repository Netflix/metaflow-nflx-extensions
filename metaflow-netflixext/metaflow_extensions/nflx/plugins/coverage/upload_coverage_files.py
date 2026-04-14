import coverage
import glob
import gzip
import hashlib
import os
import shutil
import subprocess
import uuid

from .setup_coverage import get_local_coverage_dir, get_coverage_rcfile


def hash_file(file_path: str) -> str:
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def gzip_file(source_path: str, destination_path: str) -> None:
    with open(source_path, "rb") as f_in:
        with gzip.open(destination_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def has_arcs(file_path: str) -> bool:
    try:
        cov = coverage.Coverage(data_file=file_path)
        cov.load()
        return cov.get_data().has_arcs
    except:
        # sometimes the coverage file is corrupted and cannot be loaded, eg:
        #   - doesn't seem to be a coverage data file
        #   - database disk image is malformed
        # so we just ignore it
        return False


def combine_coverage_files(local_dir):
    combined_coverage_file = f"{local_dir}/.coverage"
    if os.path.exists(combined_coverage_file):
        shutil.move(combined_coverage_file, f"{combined_coverage_file}.{uuid.uuid4()}")
    files = [f for f in glob.glob(f"{local_dir}/.coverage.*") if has_arcs(f)]
    if not files:
        return
    cov = coverage.Coverage(
        data_file=combined_coverage_file, config_file=get_coverage_rcfile()
    )
    try:
        cov.combine(data_paths=files, keep=True)
    except coverage.exceptions.NoDataError:
        return
    cov.save()
    assert os.path.exists(combined_coverage_file)


def upload_coverage_files(coverage_s3_path: str) -> None:
    coverage_s3_path = coverage_s3_path.rstrip("/")
    local_coverage_dir = get_local_coverage_dir()
    combine_coverage_files(local_coverage_dir)

    combined_coverage_file = f"{local_coverage_dir}/.coverage"
    file_hash = hash_file(combined_coverage_file)

    gzipped_file = f"{local_coverage_dir}/.coverage.{file_hash}.gz"
    gzip_file(combined_coverage_file, gzipped_file)

    subprocess.run(
        f"aws s3 cp {gzipped_file} {coverage_s3_path}/",
        shell=True,
        check=True,
        cwd=local_coverage_dir,
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python script.py <COVERAGE_S3_PATH>")
    else:
        upload_coverage_files(sys.argv[1])
