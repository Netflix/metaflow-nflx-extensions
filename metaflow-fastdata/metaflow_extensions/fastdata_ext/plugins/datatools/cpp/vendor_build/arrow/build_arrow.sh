#!/bin/sh
set -e

# Detect operating system
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "linux"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    else
        echo "unsupported"
    fi
}

# Install packages with retry for Linux apt-get
install_with_apt_retry() {
    local packages="$1"
    echo "installing $packages"
    while sudo apt-get update 2>&1 | tee /dev/stderr | grep -q "Could not get lock /var/lib/dpkg/lock-frontend" ; do
        echo "waiting for other apt-get instance to exit"
        sleep 30
    done
    while sudo apt-get -y install $packages 2>&1 | tee /dev/stderr | grep -q "Could not get lock /var/lib/dpkg/lock-frontend" ; do
        echo "waiting for other apt-get instance to exit"
        sleep 30
    done
}

# Install build tools based on OS
install_build_tools() {
    local os=$(detect_os)
    
    case $os in
        "linux")
            echo "Detected Linux - installing build tools with apt-get"
            install_with_apt_retry "build-essential ninja-build cmake"
            # Install xsimd if available in package manager
            install_with_apt_retry "libxsimd-dev" || echo "Warning: libxsimd-dev not available via apt"
            ;;
        "macos")
            echo "Detected macOS - checking for build tools"
            
            # Check for cmake
            if command -v cmake &> /dev/null; then
                echo "cmake found"
            else
                echo "cmake not found - installing with Homebrew"
                # Check if Homebrew is installed
                if ! command -v brew &> /dev/null; then
                    echo "Error: Homebrew is not installed. Please install Homebrew first:"
                    echo "/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                    exit 1
                fi
                brew install cmake
            fi
            
            # Check for ninja
            if command -v ninja &> /dev/null; then
                echo "ninja found"
            else
                echo "ninja not found - installing with Homebrew"
                # Check if Homebrew is installed
                if ! command -v brew &> /dev/null; then
                    echo "Error: Homebrew is not installed. Please install Homebrew first:"
                    echo "/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                    exit 1
                fi
                brew install ninja
            fi
            
            # Install xsimd dependency
            if command -v brew &> /dev/null; then
                echo "Installing xsimd with Homebrew..."
                brew install xsimd || echo "Warning: failed to install xsimd"
            else
                echo "Warning: Homebrew not available, xsimd may be missing"
            fi
            
            # Check if Xcode Command Line Tools are installed
            if ! xcode-select -p &> /dev/null; then
                echo "Installing Xcode Command Line Tools..."
                xcode-select --install
                echo "Please complete the Xcode Command Line Tools installation and re-run this script"
                exit 1
            fi
            ;;
        *)
            echo "Error: Unsupported operating system: $OSTYPE"
            echo "This script supports Linux and macOS only"
            exit 1
            ;;
    esac
}

# Install build tools
install_build_tools

# Setup build location
cd "$(dirname $0)"
mkdir -p build && cd build

# Function to find xsimd installation
find_xsimd_root() {
    # Common locations to check for xsimd
    local potential_paths=(
        "/opt/homebrew"           # Homebrew on Apple Silicon
        "/usr/local"              # Homebrew on Intel, MacPorts, manual installs
        "/usr"                    # System packages on Linux
        "/opt/local"              # MacPorts
        "${CONDA_PREFIX}"         # Conda environment
        "${VIRTUAL_ENV}"          # Python virtual environment
    )
    
    for path in "${potential_paths[@]}"; do
        if [ -n "$path" ] && [ -d "$path/include/xsimd" ] || [ -f "$path/include/xsimd.hpp" ]; then
            echo "$path"
            return 0
        fi
    done
    
    return 1
}

# Get build and cxx options
OPTIONS=$(cat ../build_options.cfg | grep -v ^# | sed 's/#.*//' | tr '\n' ' ')
CMAKE_BUILD_TYPE=release

# Try to find xsimd installation and add to CMAKE_PREFIX_PATH
XSIMD_ROOT=$(find_xsimd_root) || true
CMAKE_EXTRA_ARGS=""
if [ -n "$XSIMD_ROOT" ]; then
    echo "Found xsimd at: $XSIMD_ROOT"
    CMAKE_EXTRA_ARGS="-DCMAKE_PREFIX_PATH=${XSIMD_ROOT}"
else
    echo "Warning: xsimd not found in common locations, CMake will search standard paths"
fi

# Run cmake and build the libarrow.a to vendor_build/arrow/build/release
cmake ../../../vendor/arrow/cpp/ $OPTIONS $CMAKE_EXTRA_ARGS
make clean
make -j32
