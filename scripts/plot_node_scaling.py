#!/usr/bin/env python3
"""
Plot node scaling experiment results.
Shows throughput vs number of nodes to demonstrate leader bottleneck.

Usage:
    python plot_node_scaling.py
    
The script will look for experiment results in cloudlab_node_scaling directory
and generate a graph showing throughput on Y-axis vs number of nodes on X-axis.
"""

import matplotlib.pyplot as plt
import json
import os
from pathlib import Path
import numpy as np


def parse_experiment_results(experiment_dir):
    """
    Parse throughput results from experiment log files.
    
    Args:
        experiment_dir: Path to experiment directory containing results
        
    Returns:
        dict: Mapping of {num_nodes: throughput_ktps}
    """
    results = {}
    
    # Look for node_scaling_X subdirectories
    for node_count in range(4, 10):  # 4-9 nodes
        exp_name = f"node_scaling_{node_count}"
        exp_path = Path(experiment_dir) / exp_name / "0"
        
        if not exp_path.exists():
            print(f"Warning: {exp_path} does not exist, skipping")
            continue
            
        # Look for results.json or parse from logs
        results_file = exp_path / "results.json"
        if results_file.exists():
            with open(results_file, 'r') as f:
                data = json.load(f)
                if 'throughput_ktps' in data:
                    results[node_count] = data['throughput_ktps']
                    print(f"{node_count} nodes: {data['throughput_ktps']:.2f} ktps")
        else:
            # Try parsing from controller log
            controller_log = exp_path / "logs" / "controller.log"
            if controller_log.exists():
                tput = parse_throughput_from_log(controller_log)
                if tput:
                    results[node_count] = tput
                    print(f"{node_count} nodes: {tput:.2f} ktps")
    
    return results


def parse_throughput_from_log(log_file):
    """
    Parse average throughput from controller log file.
    
    Args:
        log_file: Path to controller.log
        
    Returns:
        float: Average throughput in ktps, or None if not found
    """
    throughputs = []
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                # Look for throughput measurements (adjust pattern based on your log format)
                if 'Throughput:' in line or 'tput:' in line or 'ktps' in line:
                    # Extract number before "ktps"
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if 'ktps' in part.lower() and i > 0:
                            try:
                                tput = float(parts[i-1].replace(',', ''))
                                throughputs.append(tput)
                            except ValueError:
                                pass
    except FileNotFoundError:
        return None
    
    if throughputs:
        # Return average of last 10 measurements (steady state)
        return np.mean(throughputs[-10:]) if len(throughputs) >= 10 else np.mean(throughputs)
    
    return None


def plot_node_scaling(results, output_file='node_scaling_throughput.png'):
    """
    Create a line plot showing throughput vs number of nodes.
    
    Args:
        results: dict mapping {num_nodes: throughput_ktps}
        output_file: Output filename for the plot
    """
    if not results:
        print("Error: No results to plot!")
        return
    
    # Sort by number of nodes
    nodes = sorted(results.keys())
    throughputs = [results[n] for n in nodes]
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Plot throughput vs nodes
    plt.plot(nodes, throughputs, marker='o', linewidth=2.5, markersize=10, 
             color='#2E86AB', label='PirateShip Throughput')
    
    # Add data labels on points
    for x, y in zip(nodes, throughputs):
        plt.annotate(f'{y:.1f}', 
                    xy=(x, y), 
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center',
                    fontsize=10,
                    fontweight='bold')
    
    # Formatting
    plt.xlabel('Number of Nodes', fontsize=14, fontweight='bold')
    plt.ylabel('Throughput (ktps)', fontsize=14, fontweight='bold')
    plt.title('Node Scaling: Throughput vs Number of Consensus Nodes', 
              fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.xticks(nodes)
    plt.legend(fontsize=12)
    
    # Set y-axis to start from 0 or slightly below min
    y_min = min(throughputs)
    y_max = max(throughputs)
    y_range = y_max - y_min
    plt.ylim(max(0, y_min - y_range * 0.1), y_max + y_range * 0.1)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    plt.show()


def create_summary_table(results):
    """
    Print a summary table of results.
    
    Args:
        results: dict mapping {num_nodes: throughput_ktps}
    """
    print("\n" + "="*50)
    print("NODE SCALING EXPERIMENT RESULTS")
    print("="*50)
    print(f"{'Nodes':<10} {'Throughput (ktps)':<20} {'Change':<15}")
    print("-"*50)
    
    nodes = sorted(results.keys())
    prev_tput = None
    
    for node_count in nodes:
        tput = results[node_count]
        if prev_tput is None:
            change_str = "-"
        else:
            change_pct = ((tput - prev_tput) / prev_tput) * 100
            change_str = f"{change_pct:+.1f}%"
        
        print(f"{node_count:<10} {tput:<20.2f} {change_str:<15}")
        prev_tput = tput
    
    print("="*50)
    
    # Calculate scaling efficiency
    if len(nodes) >= 2:
        base_nodes = nodes[0]
        base_tput = results[base_nodes]
        
        print(f"\nScaling efficiency (relative to {base_nodes} nodes):")
        for node_count in nodes[1:]:
            ideal_tput = base_tput * (node_count / base_nodes)
            actual_tput = results[node_count]
            efficiency = (actual_tput / ideal_tput) * 100
            print(f"  {node_count} nodes: {efficiency:.1f}% (actual: {actual_tput:.1f} ktps, ideal: {ideal_tput:.1f} ktps)")


def main():
    # Look for cloudlab_node_scaling directory
    base_dir = Path(__file__).parent.parent
    experiment_dir = base_dir / "cloudlab_node_scaling"
    
    # Check if directory exists
    if not experiment_dir.exists():
        print(f"Error: Experiment directory not found: {experiment_dir}")
        print("Please run the node scaling experiments first.")
        return
    
    # Find most recent experiment run
    timestamp_dirs = sorted([d for d in experiment_dir.iterdir() if d.is_dir()], reverse=True)
    if timestamp_dirs:
        experiment_dir = timestamp_dirs[0] / "experiments"
        print(f"Using experiment directory: {experiment_dir}")
    
    # Parse results
    print("\nParsing experiment results...")
    results = parse_experiment_results(experiment_dir)
    
    if not results:
        print("\nNo results found. Make sure experiments have been run.")
        print("Expected directory structure: cloudlab_node_scaling/TIMESTAMP/experiments/node_scaling_X/0/")
        return
    
    # Create summary table
    create_summary_table(results)
    
    # Generate plot
    print("\nGenerating plot...")
    output_file = base_dir / "node_scaling_throughput.png"
    plot_node_scaling(results, output_file=str(output_file))


if __name__ == "__main__":
    main()
