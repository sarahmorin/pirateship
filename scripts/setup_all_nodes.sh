#!/bin/bash
# Deploy and run setup on all CloudLab nodes sequentially
# Use this for better error visibility during initial setup

SSH_USER="nurzhana"

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

echo "=========================================="
echo "CloudLab Sequential Node Setup"
echo "=========================================="
echo "Total nodes: ${#NODES[@]}"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="$SCRIPT_DIR/cloudlab_node_setup.sh"

if [ ! -f "$SETUP_SCRIPT" ]; then
    echo "Error: Setup script not found at $SETUP_SCRIPT"
    exit 1
fi

success_count=0
fail_count=0
failed_nodes=()

for node in "${NODES[@]}"; do
    node_name=$(echo $node | cut -d'.' -f1)
    
    echo ""
    echo "=========================================="
    echo "Setting up: $node_name ($node)"
    echo "=========================================="
    
    # Copy setup script
    echo "Copying setup script..."
    if scp -o StrictHostKeyChecking=no "$SETUP_SCRIPT" ${SSH_USER}@${node}:~/cloudlab_node_setup.sh; then
        echo "✓ Script copied"
    else
        echo "✗ Failed to copy script to $node_name"
        fail_count=$((fail_count + 1))
        failed_nodes+=("$node_name (copy failed)")
        continue
    fi
    
    # Run setup script
    echo ""
    echo "Running setup on $node_name..."
    echo "---"
    
    if ssh -o StrictHostKeyChecking=no ${SSH_USER}@${node} "chmod +x ~/cloudlab_node_setup.sh && ~/cloudlab_node_setup.sh"; then
        echo "---"
        echo "✓ $node_name setup completed successfully"
        success_count=$((success_count + 1))
    else
        echo "---"
        echo "✗ $node_name setup failed"
        fail_count=$((fail_count + 1))
        failed_nodes+=("$node_name (setup failed)")
        
        # Ask if user wants to continue
        echo ""
        read -p "Continue with remaining nodes? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Setup aborted by user."
            break
        fi
    fi
done

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo "Successful: $success_count/${#NODES[@]}"
echo "Failed: $fail_count/${#NODES[@]}"

if [ $fail_count -gt 0 ]; then
    echo ""
    echo "Failed nodes:"
    for failed in "${failed_nodes[@]}"; do
        echo "  - $failed"
    done
fi

echo ""
echo "Next steps:"
echo "  1. Verify setup by SSHing to a node and running: rustc --version"
echo "  2. Deploy PirateShip code using the deployment script"
echo ""
