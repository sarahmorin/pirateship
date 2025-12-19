# Running Experiments on CloudLab

This document describes how to run PirateShip / DAG-PirateShip experiments on **CloudLab**.

---

## 1. Reserve Nodes on CloudLab

1. Create a CloudLab experiment and reserve the desired number of nodes.
2. Ensure you can SSH into all nodes from your local machine.
3. Collect:
   - SSH access (your public key installed on all nodes)
   - Fully-qualified hostnames for each node

---

## 2. Configure Node Hostnames

Edit the `NODES` array in both:
- `scripts/setup_all_nodes.sh`
- `scripts/distribute_ssh_keys.sh`

Use the following format:

    
    NODES=(
        "hp126.utah.cloudlab.us" # node0
        "hp134.utah.cloudlab.us" # node1
        "hp152.utah.cloudlab.us" # node2
        "hp091.utah.cloudlab.us" # node3
        "hp138.utah.cloudlab.us" # node4
        "hp097.utah.cloudlab.us" # node5
        "hp125.utah.cloudlab.us" # node6
        "hp156.utah.cloudlab.us" # node7
        "hp155.utah.cloudlab.us" # node8
        "hp090.utah.cloudlab.us" # node9
        "hp160.utah.cloudlab.us" # node10
    )


## 3. Set Up All Nodes

Activate the Python virtual environment and run the setup scripts:


    source .venv-wsl/bin/activate

    chmod +x scripts/setup_all_nodes.sh scripts/distribute_ssh_keys.sh

    ./scripts/setup_all_nodes.sh
    ./scripts/distribute_ssh_keys.sh

These scripts:

- Install system dependencies
- Build binaries
- Synchronize SSH keys across nodes
- Ensure passwordless SSH between all replicas

---

## 4. Configure Experiment TOML Files

Edit the relevant `.toml` files under `experiments/` to:

- Match the number of nodes
- Use the correct hostnames
- Adjust workload parameters if needed

---

## 5. Run Experiments

Run experiments using the experiment driver script:


    python scripts -c experiments/cloudlab_protocol_comparison.toml
    python scripts -c experiments/cloudlab_node_scaling_trajectory.toml
    python scripts -c experiments/cloudlab_dag_pirateship_sweep.toml
    python scripts -c experiments/cloudlab_network_blip.toml
    python scripts -c experiments/cloudlab_network_blip_dag.toml

Each command launches a full multi-node experiment on CloudLab and collects logs and metrics automatically.

---

## Notes

- Ensure all nodes are reachable before starting experiments.
- Experiments assume a clean state on all nodes.
- Network blip experiments require `iptables` and root access (handled by the setup scripts).
