#!/bin/bash
# CloudLab node setup script
# This script installs all dependencies needed to build and run PirateShip

set -e  # Exit on error

echo "=========================================="
echo "CloudLab Node Setup"
echo "=========================================="
echo "Hostname: $(hostname)"
echo "User: $(whoami)"
echo ""

# Update package lists
echo "[1/6] Updating package lists..."
sudo apt-get update -qq

# Install basic dependencies
echo "[2/6] Installing build tools and dependencies..."
sudo apt-get install -y \
    build-essential \
    cmake \
    clang \
    llvm \
    pkg-config \
    jq \
    protobuf-compiler \
    ca-certificates \
    curl \
    libssl-dev \
    net-tools \
    git \
    tmux \
    rsync \
    > /dev/null 2>&1

echo "✓ Build tools installed"

# Install Rust
echo "[3/6] Installing Rust..."
if [ ! -d "$HOME/.cargo" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    echo "✓ Rust installed"
else
    echo "✓ Rust already installed"
fi

# Source Rust environment
source $HOME/.cargo/env

# Configure bash to always source Rust env
echo "[4/6] Configuring shell environment..."
if ! grep -q "\.cargo/env" $HOME/.bashrc; then
    echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> $HOME/.bashrc
    echo "✓ Added Rust to .bashrc"
else
    echo "✓ Rust already in .bashrc"
fi

# Ensure .ssh directory exists with correct permissions
echo "[5/6] Setting up SSH directory..."
mkdir -p $HOME/.ssh
chmod 700 $HOME/.ssh
echo "✓ SSH directory configured"

# Create data directory for experiment logs/storage
echo "[6/6] Creating data directory..."
sudo mkdir -p /data
sudo chown -R $(whoami) /data
sudo chmod 755 /data
echo "✓ Data directory created at /data"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo "Rust version: $(rustc --version)"
echo "Cargo version: $(cargo --version)"
echo ""
echo "Node is ready for PirateShip experiments."
echo ""

