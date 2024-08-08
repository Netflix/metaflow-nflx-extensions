#!/usr/bin/env bash

python_executable=""
python_version=""
file_opt=""
file_name=""
alias_name=""
using_env=""
declare -A check_versions

my_dir=$(dirname "$0")
my_dir=$(realpath "$my_dir")

env_dir="$my_dir/environments"
check_dir="$my_dir/checks"

usage() {
    echo "Usage: $0 -e python_executable -p python_version [-r req_file_name] [-f yml_file_name] [-u using_env_name] [-a alias]"
}

exit_bad_cmd() {
    usage
    exit 1
}

script_output=""

cleanup() {
    if [[ -n "${script_output}" ]]; then
        rm "${script_output}"
    fi
}

trap cleanup EXIT

# A simple function to compare version strings x.y.z
compare_versions() {
    local version1=$1
    local version2=$2

    if [[ "$version1" == "$version2" ]]; then
        echo 0
        return
    fi

    local ver1_arr
    local ver2_arr
    IFS='.' read -ra ver1_arr <<<"$version1"
    IFS='.' read -ra ver2_arr <<<"$version2"

    local max_components=$((${#ver1_arr[@]} > ${#ver2_arr[@]} ? ${#ver1_arr[@]} : ${#ver2_arr[@]}))

    for ((i = 0; i < max_components; i++)); do
        local component1=${ver1_arr[$i]:-0}
        local component2=${ver2_arr[$i]:-0}
        # Ignore pre/post/dev markers
        if [[ "$component1" == "dev*" || "$component1" == "pre*" || "$component1" == "post*" ||
            "$component2" == "dev*" || "$component2" == "pre*" || "$component2" == "post*" ]]; then
            continue
        fi
        if ((component1 < component2)); then
            echo 1
            return
        elif ((component1 > component2)); then
            echo 2
            return
        fi
    done
    echo 0
}

while getopts "e:p:r:f:u:a:" o; do
    case "${o}" in
    e)
        if [[ -z "$python_executable" ]]; then
            python_executable=$OPTARG
        else
            echo "Error: Cannot specify multiple -e options"
            exit_bad_cmd
        fi
        ;;

    p)
        if [[ -z "$python_version" ]]; then
            python_version=$OPTARG
        else
            echo "Error: Cannot specify multiple -p options"
            exit_bad_cmd
        fi
        ;;
    r)
        if [[ -z "$file_name" ]]; then
            file_name=$OPTARG
            file_opt="-r"
        else
            echo "Error: Can only specify one of -r or -f and only once"
            exit_bad_cmd
        fi
        ;;
    f)
        if [[ -z "$file_name" ]]; then
            file_name=$OPTARG
            file_opt="-f"
        else
            echo "Error: Can only specify one of -r or -f and only once"
            exit_bad_cmd
        fi
        ;;
    u)
        if [[ -z "$using_env" ]]; then
            using_env=$OPTARG
        else
            echo "Error: Cannot specify multiple -u options"
            exit_bad_cmd
        fi
        ;;
    a)
        if [[ -z "$alias_name" ]]; then
            alias_name=$OPTARG
        else
            echo "Error: Cannot specify multiple -a options"
            exit_bad_cmd
        fi
        ;;
    :)
        echo "Error: -${OPTARG} requires an argument."
        exit_bad_cmd
        ;;
    \?)
        echo "Cannot parse command line"
        exit_bad_cmd
        ;;
    esac
done

if [[ -z "$python_executable" ]]; then
    echo "Error: Need to specify python executable"
    exit_bad_cmd
fi

# Get all the checks we need to perform
if [[ -e "$check_dir/$file_name.check" ]]; then
    while IFS=',' read -r pkg min max req_py_version; do
        # Skip empty lines
        if [[ -z "$pkg" ]]; then
            continue
        fi
        if [[ -n "$req_py_version" && "$req_py_version" != "$python_version" ]]; then
            continue
        fi
        if [[ -v check_versions["$pkg"] ]]; then
            echo "Error: Cannot specify a check on $pkg more than once"
            exit_bad_cmd
        fi
        if [[ -z "$min" ]]; then
            min="0"
        fi
        if [[ -z "$max" ]]; then
            max="1000000"
        fi
        check_versions["$pkg"]="$min,$max"
    done <<<"$(cat $check_dir/$file_name.check)"
fi

export METAFLOW_DEBUG_CONDA=1
# Form the command line for resolution
script_output=$(mktemp)
cmd="$python_executable -m metaflow.cmd.main_cli environment --quiet --quiet-file-output ${script_output} resolve "

if [[ -z "$python_version" ]]; then
    echo "Error: requires -p <python_version>"
    exit_bad_cmd
else
    if [[ "$python_version" == "file" ]]; then
        cmd="$cmd $file_opt $env_dir/$file_name"
    else
        cmd="$cmd --python $python_version $file_opt $env_dir/$file_name"
    fi
fi

if [[ -n "$using_env" ]]; then
    cmd="$cmd --using $using_env"
fi

if [[ -n "$alias_name" ]]; then
    cmd="$cmd --alias $alias_name"
fi

echo "Resolving environment using $cmd"

if $cmd; then
    env_id=$(
        sed -n '2p' "${script_output}" | cut -d' ' -f1-2 | tr ' ' :
    )
    echo "Successfully resolved environment and got ${env_id}"
else
    echo "Error: Failed to resolve using $cmd"
    exit 2
fi

# Create the environment
cmd="$python_executable -m metaflow.cmd.main_cli environment --quiet --quiet-file-output ${script_output} create --force"
if [[ -n "$alias_name" ]]; then
    cmd="$cmd $alias_name"
else
    cmd="$cmd $env_id"
fi

echo "Creating environment using $cmd"
if $cmd; then
    python_binary=$(IFS=' ' read -r bin <"${script_output}" && echo "$bin")
    echo "Successfully created environment with binary $python_binary"
else
    echo "Error: Failed to create environment using $cmd"
    exit 2
fi

# Verify using pip freeze to see if all the packages we want are actually there
freeze_output=$("$python_binary" -m pip list --format freeze)

declare -A all_pkgs
while read -r line; do
    split_line=$(echo "$line" | sed -e 's/\([^=]*\)==\(.*\)/\1 \2/')
    read -r pkg_name pkg_version <<<"$split_line"
    all_pkgs["$pkg_name"]="$pkg_version"
done <<<"$freeze_output"

declare -a all_errors
for key in "${!check_versions[@]}"; do
    version_check_str="${check_versions[$key]}"
    min_version="${version_check_str%%,*}"
    max_version="${version_check_str#*,}"
    if [[ "$min_version" == "-1" ]]; then
        if [[ -v all_pkgs["$key"] ]]; then
            all_errors+=("Expected package $key to be absent but found")
        fi
    else
        if [[ ! -v all_pkgs["$key"] ]]; then
            all_errors+=("Expected package $key but not found")
        else
            cur_version="${all_pkgs["$key"]}"
            smaller_cmp=$(compare_versions "$min_version" "$cur_version")
            larger_cmp=$(compare_versions "$cur_version" "$max_version")
            echo "For $key got min:$min_version; max:$max_version; cur:$cur_version"
            if [[ "$smaller_cmp" -gt 1 ]]; then
                all_errors+=("Expected $key to have mimimum version $min_version but found $cur_version")
            fi
            if [[ "$larger_cmp" -gt 1 ]]; then
                all_errors+=("Expected $key to have maximum version $max_version but found $cur_version")
            fi
        fi
    fi
done

# Print some helpful information
echo "** Pip freeze output **"
echo "$freeze_output"
echo ""
echo ""
echo "** Environment info output **"
env_output=$("$python_executable" -m metaflow.cmd.main_cli environment show "$env_id")
echo "$env_output"

if [[ "${#all_errors[@]}" -gt 0 ]]; then
    echo "Found errors: "
    for err in "${all_errors[@]}"; do
        echo "    $err"
    done
    exit 2
fi
