#!/bin/bash
# CloudLab Node Setup Script for PirateShip
# This script sets up all dependencies needed to build and run PirateShip

set -e  # Exit on error

echo "=========================================="
echo "PirateShip CloudLab Node Setup"
echo "=========================================="
echo ""

# Update package lists
echo "[1/6] Updating package lists..."
sudo apt-get update

# Install basic build tools
echo "[2/6] Installing build essentials..."
sudo apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    curl \
    wget \
    cmake \
    ninja-build \
    protobuf-compiler \
    libprotobuf-dev

# Install LLVM and Clang (specific version to avoid compatibility issues)
echo "[3/6] Installing LLVM and Clang..."
# Install LLVM 14 (stable and well-supported)
sudo apt-get install -y \
    llvm-14 \
    llvm-14-dev \
    llvm-14-runtime \
    clang-14 \
    libclang-14-dev \
    lld-14

# Set up environment variables for LLVM/Clang
echo "[4/6] Configuring LLVM environment..."
export LLVM_SYS_140_PREFIX=/usr/lib/llvm-14
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
export CLANG_PATH=/usr/bin/clang-14

# Create symlinks for clang/clang++ if they don't exist
if [ ! -f "/usr/bin/clang" ]; then
    sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-14 100
    sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-14 100
    echo "✓ Created clang symlinks"
fi

# Add to bashrc for persistence
if ! grep -q "LLVM_SYS_140_PREFIX" ~/.bashrc; then
    cat >> ~/.bashrc << 'EOF'

# LLVM/Clang configuration for Rust builds
export LLVM_SYS_140_PREFIX=/usr/lib/llvm-14
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
export CLANG_PATH=/usr/bin/clang-14
export CC=clang-14
export CXX=clang++-14
EOF
    echo "✓ Added LLVM environment variables to ~/.bashrc"
fi

# Install Rust
echo "[5/6] Installing Rust..."
if [ ! -d "$HOME/.cargo" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source $HOME/.cargo/env
    echo "✓ Rust installed successfully"
else
    echo "✓ Rust already installed, updating..."
    source $HOME/.cargo/env
    rustup update stable
fi

# Verify Rust installation
rustc --version
cargo --version

# Create /data directory with correct permissions
echo "[6/6] Setting up /data directory..."
if [ ! -d "/data" ]; then
    sudo mkdir -p /data
    sudo chown $USER:$(id -gn) /data
    echo "✓ Created /data directory"
else
    echo "✓ /data directory already exists"
    # Ensure correct ownership
    sudo chown $USER:$(id -gn) /data
fi

# Verify installations
echo ""
echo "=========================================="
echo "Verification"
echo "=========================================="
echo "GCC version:"
gcc --version | head -1
echo ""
echo "Clang version:"
clang-14 --version | head -1
echo ""
echo "LLVM version:"
llvm-config-14 --version
echo ""
echo "Rust version:"
rustc --version
echo ""
echo "Protobuf compiler:"
protoc --version
echo ""

# Test libclang
echo "Testing libclang..."
if [ -f "/usr/lib/llvm-14/lib/libclang.so" ]; then
    echo "✓ libclang found at: /usr/lib/llvm-14/lib/libclang.so"
else
    echo "⚠ Warning: libclang.so not found at expected location"
    echo "Searching for libclang..."
    find /usr -name "libclang.so*" 2>/dev/null | head -5
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Environment variables set:"
echo "  LLVM_SYS_140_PREFIX=$LLVM_SYS_140_PREFIX"
echo "  LIBCLANG_PATH=$LIBCLANG_PATH"
echo "  CLANG_PATH=$CLANG_PATH"
echo ""
echo "Next steps:"
echo "  1. Log out and log back in (or run: source ~/.bashrc)"
echo "  2. Clone pirateship repository"
echo "  3. Run: cargo build --release"
echo ""
