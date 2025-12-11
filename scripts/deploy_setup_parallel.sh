#!/bin/bash
# Deploy setup script to all CloudLab nodes and execute in parallel

set -e

# New 10-node CloudLab deployment
NODES=(
    "hp182.utah.cloudlab.us"  # client node
    "hp113.utah.cloudlab.us"  # consensus node 1
    "hp161.utah.cloudlab.us"  # consensus node 2
    "hp171.utah.cloudlab.us"  # consensus node 3
    "hp083.utah.cloudlab.us"  # consensus node 4
    "hp087.utah.cloudlab.us"  # consensus node 5
    "hp103.utah.cloudlab.us"  # consensus node 6
    "hp169.utah.cloudlab.us"  # consensus node 7
    "hp196.utah.cloudlab.us"  # consensus node 8
    "hp115.utah.cloudlab.us"  # consensus node 9
)

SSH_USER="nurzhana"
SCRIPT_PATH="$(dirname "$0")/cloudlab_node_setup.sh"

echo "=========================================="
echo "Parallel CloudLab Node Setup"
echo "=========================================="
echo "Nodes to setup: ${#NODES[@]}"
echo ""

# Check if setup script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Error: Setup script not found at $SCRIPT_PATH"
    exit 1
fi

# Function to setup a single node
setup_node() {
    local node=$1
    local node_name=$(echo $node | cut -d'.' -f1)
    
    echo "[$node_name] Starting setup..."
    
    # Copy setup script to node
    if scp "$SCRIPT_PATH" ${SSH_USER}@${node}:~/cloudlab_node_setup.sh 2>&1 | grep -v "Warning: Permanently added"; then
        echo "[$node_name] ✓ Script copied"
    else
        echo "[$node_name] ✗ Failed to copy script"
        return 1
    fi
    
    # Execute setup script on node
    if ssh ${SSH_USER}@${node} "chmod +x ~/cloudlab_node_setup.sh && ~/cloudlab_node_setup.sh" > /tmp/setup_${node_name}.log 2>&1; then
        echo "[$node_name] ✓ Setup completed successfully"
        return 0
    else
        echo "[$node_name] ✗ Setup failed (check /tmp/setup_${node_name}.log for details)"
        return 1
    fi
}

# Export function for parallel execution
export -f setup_node
export SSH_USER
export SCRIPT_PATH

# Run setup on all nodes in parallel
echo "Starting parallel setup on all nodes..."
echo ""

# Create array to track background jobs
declare -a pids=()

# Launch setup on each node in background
for node in "${NODES[@]}"; do
    setup_node "$node" &
    pids+=($!)
done

# Wait for all background jobs to complete
echo "Waiting for all setups to complete..."
echo ""

failed=0
for i in "${!pids[@]}"; do
    if wait ${pids[$i]}; then
        : # Success, do nothing
    else
        failed=$((failed + 1))
    fi
done

echo ""
echo "=========================================="
echo "Setup Summary"
echo "=========================================="
echo "Total nodes: ${#NODES[@]}"
echo "Successful: $((${#NODES[@]} - failed))"
echo "Failed: $failed"
echo ""

if [ $failed -eq 0 ]; then
    echo "✓ All nodes setup successfully!"
    echo ""
    echo "Next steps:"
    echo "  1. Deploy PirateShip code to nodes"
    echo "  2. Run experiments using: python -m scripts --config experiments/cloudlab_node_scaling.toml"
else
    echo "⚠ Some nodes failed to setup. Check logs in /tmp/setup_*.log"
    echo ""
    echo "Failed node logs:"
    for node in "${NODES[@]}"; do
        node_name=$(echo $node | cut -d'.' -f1)
        if [ -f "/tmp/setup_${node_name}.log" ]; then
            if grep -q "Error\|Failed\|error:" "/tmp/setup_${node_name}.log" 2>/dev/null; then
                echo "  - /tmp/setup_${node_name}.log"
            fi
        fi
    done
fi

echo ""
