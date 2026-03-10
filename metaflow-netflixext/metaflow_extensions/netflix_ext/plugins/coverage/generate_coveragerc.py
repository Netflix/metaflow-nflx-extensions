import configparser
import coverage
import re
import os
from metaflow_extensions.netflix_ext.plugins.coverage.setup_coverage import get_coverage_rcfile


def get_tmp_dirs(pattern, coverage_rcfile, data_file=".coverage"):
    cov = coverage.Coverage(data_file=data_file, config_file=coverage_rcfile)
    cov.load()
    cov_data = cov.get_data()
    files_covered = cov_data.measured_files()
    tmp_dirs = set()
    pattern = re.compile(pattern)
    for file_path in files_covered:
        match = pattern.match(file_path)
        if match:
            tmp_dirs.add(match.group(1))
    return list(tmp_dirs)


def remap_tmp_dirs(cov, coverage_rcfile, data_file):
    """
    We need to update the [paths] config to remap all the temporary directories.
    This function iterates over all the files and uses a regex match to
    find the relevent paths that need remapping.
    """
    config = cov.config
    trampoline_dirs = get_tmp_dirs(
        r"^(/data/tmp/[^/]+/)([^/]+\.py)$", coverage_rcfile, data_file
    )
    config.paths["_escape_trampolines"] = ["_escape_trampolines"] + trampoline_dirs
    ttk_dirs = get_tmp_dirs(
        r"(?P<dir>.*?/_ttk_[^/]+)/ray_train_func\.py$", coverage_rcfile, data_file
    )
    print(f"DEBUG: Found {len(ttk_dirs)} TTK directories: {ttk_dirs}")
    config.paths["trainingplatform"] = [
        ".mf_code/metaflow_extensions/nflx/plugins/trainingplatform"
    ] + ttk_dirs
    ray_dirs = get_tmp_dirs(
        r"(?P<dir>.*?/_ray_pkg_[^/]+)/.+", coverage_rcfile, data_file
    )
    config.paths["ray"] = ["."] + ray_dirs
    # Initialize training_platform if it doesn't exist, then add ray_dirs
    if "training_platform" not in config.paths:
        config.paths["training_platform"] = []
    config.paths["training_platform"] = config.paths["training_platform"] + ray_dirs
    formatted_dirs = get_tmp_dirs(
        r"(?P<dir>.*?)/[^/]+_(check|test)_flow\.py$", coverage_rcfile, data_file
    )
    config.paths["_formatted_flows"] = ["_formatted_flows"] + formatted_dirs


def save_updated_coveragerc(config, coverage_rcfile, output_file):
    # Read template as text to preserve all sections and comments
    with open(coverage_rcfile, "r") as f:
        template_content = f.read()

    # Parse template to get existing paths
    config_parser = configparser.ConfigParser()
    config_parser.read_string(template_content)

    # Update [paths] section: preserve template paths, add/update dynamic ones
    if not config_parser.has_section("paths"):
        config_parser.add_section("paths")

    # Merge: Start with all template paths, then add/update dynamic ones
    merged_paths = {}

    # First, preserve all template paths
    if config_parser.has_section("paths"):
        for key in config_parser.options("paths"):
            value = config_parser.get("paths", key)
            # ConfigParser returns multi-line values with newlines already
            merged_paths[key] = value

    # Then add or override with dynamic paths from config.paths
    for key, path_list in config.paths.items():
        # Skip empty path lists to avoid IndexError in coverage.py
        if path_list:
            merged_paths[key] = "\n".join(path_list)

    # Write merged paths back (skip empty values)
    for key, value in merged_paths.items():
        if value.strip():  # Only write non-empty paths
            config_parser.set("paths", key, value)

    with open(output_file, "w") as configfile:
        config_parser.write(configfile)
    print(f"Updated .coveragerc file written to {output_file}")


if __name__ == "__main__":
    cov = coverage.Coverage(data_file=".coverage", config_file=get_coverage_rcfile())
    remap_tmp_dirs(cov, get_coverage_rcfile(), ".coverage")
    save_updated_coveragerc(cov.config, get_coverage_rcfile(), ".coveragerc")
