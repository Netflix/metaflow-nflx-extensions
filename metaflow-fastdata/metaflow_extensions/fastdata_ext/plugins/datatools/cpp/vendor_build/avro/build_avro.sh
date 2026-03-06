#!/bin/sh
set -e

readonly JANSSON_VERSION_CODE=2.13
readonly JANSSON_VERSION=jansson-${JANSSON_VERSION_CODE}

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

# Install packages based on OS
install_dependencies() {
    local os=$(detect_os)
    
    case $os in
        "linux")
            echo "Detected Linux - using apt-get"
            # Install cmake if not present
            if which cmake > /dev/null 2>&1; then
                echo "cmake found"
            else
                echo "installing cmake"
                install_with_apt_retry "cmake"
            fi
            
            # Install other dependencies
            install_with_apt_retry "pkg-config libsasl2-dev zlib1g-dev"
            install_with_apt_retry "libjansson-dev"
            install_with_apt_retry "liblzma5 liblzma-dev"
            install_with_apt_retry "libsnappy-dev"
            ;;
        "macos")
            echo "Detected macOS - checking dependencies"
            
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
            
            # Install other dependencies with Homebrew (check if brew is available)
            if command -v brew &> /dev/null; then
                echo "Installing other dependencies with Homebrew..."
                brew install pkg-config cyrus-sasl jansson xz snappy || true
            else
                echo "Warning: Homebrew not available, some dependencies may be missing"
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

# Install dependencies
install_dependencies

# Setup build location and release path
cd "$(dirname $0)"
mkdir -p build && cd build
mkdir -p release
REL_PATH="$(pwd)/release"
export REL_PATH

# Get and build https://digip.org/jansson/
wget --no-check-certificate https://github.com/akheron/jansson/releases/download/v${JANSSON_VERSION_CODE}/${JANSSON_VERSION}.tar.gz
tar -xf ${JANSSON_VERSION}.tar.gz
mkdir -p jansson_build && cd jansson_build
cmake ../${JANSSON_VERSION}/ \
      -DJANSSON_BUILD_DOCS=OFF \
      -DJANSSON_EXAMPLES=OFF\
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="${REL_PATH}"
PKG_CONFIG_PATH=$(pwd)
export PKG_CONFIG_PATH
# Fix jansson.pc for cross-platform compatibility
if [ "$(detect_os)" = "macos" ]; then
    sed -i '' "1s+=.*+=${PKG_CONFIG_PATH}+" jansson.pc
else
    sed -i "1s+=.*+=${PKG_CONFIG_PATH}+" jansson.pc
fi
make -j32
make install
cd ../

# Run cmake and build the libarrow.a to vendor_build/arrow/build/release
# Set appropriate flags based on OS
OS=$(detect_os)

# Function to find snappy installation
find_snappy_root() {
    # Common locations to check for snappy
    local potential_paths=(
        "/opt/homebrew"           # Homebrew on Apple Silicon
        "/usr/local"              # Homebrew on Intel, MacPorts, manual installs
        "/usr"                    # System packages on Linux
        "/opt/local"              # MacPorts
        "${CONDA_PREFIX}"         # Conda environment
        "${VIRTUAL_ENV}"          # Python virtual environment
    )
    
    for path in "${potential_paths[@]}"; do
        if [ -n "$path" ] && [ -f "$path/include/snappy-c.h" ] && [ -f "$path/lib/libsnappy.a" -o -f "$path/lib/libsnappy.so" -o -f "$path/lib/libsnappy.dylib" ]; then
            echo "$path"
            return 0
        fi
    done
    
    return 1
}

# Try to find snappy installation
SNAPPY_ROOT=$(find_snappy_root) || true
CMAKE_ARGS=""
if [ -n "$SNAPPY_ROOT" ]; then
    echo "Found snappy at: $SNAPPY_ROOT"
    echo "Debug: Setting Snappy_DIR=${SNAPPY_ROOT}/lib/cmake/Snappy"
    CMAKE_ARGS="-DSnappy_DIR=${SNAPPY_ROOT}/lib/cmake/Snappy"

    # Also set CMAKE_PREFIX_PATH as fallback
    export CMAKE_PREFIX_PATH="${SNAPPY_ROOT}:${CMAKE_PREFIX_PATH:-}"
    echo "Debug: CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}"

    # Set include and library paths explicitly
    export CFLAGS="${CFLAGS:--Wno-deprecated-non-prototype -Wno-int-conversion} -I${SNAPPY_ROOT}/include"
    export LDFLAGS="${LDFLAGS:-} -L${SNAPPY_ROOT}/lib"
else
    echo "Warning: snappy not found in common locations, CMake will search standard paths"
fi

if [ "$OS" = "linux" ]; then
    cmake ../../../vendor/avro/lang/c/ \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_FLAGS="-I/usr/include -I/usr/include/x86_64-linux-gnu" \
          -DCMAKE_C_FLAGS="-I/usr/include -I/usr/include/x86_64-linux-gnu" \
          -DCMAKE_INSTALL_PREFIX="${REL_PATH}" \
          ${CMAKE_ARGS}
else
    # macOS: CFLAGS is already set with snappy include path if found
    cmake ../../../vendor/avro/lang/c/ \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_C_FLAGS="${CFLAGS}" \
          -DCMAKE_INSTALL_PREFIX="${REL_PATH}" \
          ${CMAKE_ARGS}
fi
make clean
make -j32
make install
