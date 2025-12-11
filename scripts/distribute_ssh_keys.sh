#!/bin/bash
# Distribute SSH key to all CloudLab nodes for inter-node communication

SSH_USER="nurzhana"
# Detect if running from WSL or native Linux
if [ -f "/mnt/c/Users/nurzh/.ssh/id_ed25519" ]; then
    SSH_KEY_LOCAL="/mnt/c/Users/nurzh/.ssh/id_ed25519"
elif [ -f "$HOME/.ssh/id_ed25519" ]; then
    SSH_KEY_LOCAL="$HOME/.ssh/id_ed25519"
else
    echo "Error: SSH key not found in /mnt/c/Users/nurzh/.ssh/ or $HOME/.ssh/"
    exit 1
fi

# New 10-node CloudLab deployment
NODES=(
    "hp119.utah.cloudlab.us"
    "hp084.utah.cloudlab.us"
    "hp081.utah.cloudlab.us"
    "hp116.utah.cloudlab.us"
    "hp102.utah.cloudlab.us"
    "hp106.utah.cloudlab.us"
    "hp107.utah.cloudlab.us"
    "hp105.utah.cloudlab.us"
    "hp088.utah.cloudlab.us"
    "hp118.utah.cloudlab.us"
)

# Use first node as distribution hub
HUB_NODE="${NODES[0]}"

echo "=========================================="
echo "SSH Key Distribution"
echo "=========================================="
echo "Hub node: $HUB_NODE"
echo "Target nodes: ${#NODES[@]}"
echo ""

# Step 1: Copy SSH key from local to hub node
echo "[Step 1] Copying SSH key to hub node ($HUB_NODE)..."
if scp -o StrictHostKeyChecking=no "$SSH_KEY_LOCAL" ${SSH_USER}@${HUB_NODE}:/users/${SSH_USER}/.ssh/id_ed25519; then
    echo "✓ SSH key copied to hub node"
else
    echo "✗ Failed to copy SSH key to hub node"
    exit 1
fi

# Set correct permissions on hub node
echo "[Step 2] Setting permissions on hub node..."
if ssh -o StrictHostKeyChecking=no ${SSH_USER}@${HUB_NODE} "chmod 600 /users/${SSH_USER}/.ssh/id_ed25519"; then
    echo "✓ Permissions set on hub node"
else
    echo "✗ Failed to set permissions on hub node"
    exit 1
fi

echo ""
echo "[Step 3] Distributing SSH key from hub to all other nodes..."
echo ""

# Step 3: From hub node, distribute to all other nodes
success=0
failed=0

for node in "${NODES[@]}"; do
    node_name=$(echo $node | cut -d'.' -f1)
    
    # Skip hub node (already has key)
    if [ "$node" = "$HUB_NODE" ]; then
        echo "  [$node_name] Skipping (hub node)"
        success=$((success + 1))
        continue
    fi
    
    echo -n "  [$node_name] Copying key... "
    
    # Copy key from hub to target node
    if ssh -o StrictHostKeyChecking=no ${SSH_USER}@${HUB_NODE} \
        "scp -o StrictHostKeyChecking=no /users/${SSH_USER}/.ssh/id_ed25519 ${SSH_USER}@${node}:/users/${SSH_USER}/.ssh/id_ed25519" &>/dev/null; then
        
        # Set permissions on target node
        if ssh -o StrictHostKeyChecking=no ${SSH_USER}@${HUB_NODE} \
            "ssh -o StrictHostKeyChecking=no ${SSH_USER}@${node} 'chmod 600 /users/${SSH_USER}/.ssh/id_ed25519'" &>/dev/null; then
            echo "✓"
            success=$((success + 1))
        else
            echo "✗ (chmod failed)"
            failed=$((failed + 1))
        fi
    else
        echo "✗ (copy failed)"
        failed=$((failed + 1))
    fi
done

echo ""
echo "=========================================="
echo "Distribution Complete"
echo "=========================================="
echo "Successful: $success/${#NODES[@]}"
echo "Failed: $failed/${#NODES[@]}"
echo ""

if [ $failed -eq 0 ]; then
    echo "✓ SSH keys distributed to all nodes!"
    echo ""
    echo "Verification: SSH from any node to any other node should work without password"
    echo "  Example: ssh ${SSH_USER}@${HUB_NODE} 'ssh ${SSH_USER}@hp161.utah.cloudlab.us hostname'"
else
    echo "⚠ Some nodes failed. You may need to manually copy keys to failed nodes."
fi

echo ""
