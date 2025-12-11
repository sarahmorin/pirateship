#!/usr/bin/env python3
"""
Simple node scaling plotter - manually enter throughput values.

Usage:
    python plot_node_scaling_simple.py
"""

import matplotlib.pyplot as plt
import numpy as np


def plot_node_scaling_results():
    """
    Plot node scaling results with manual data entry.
    Edit the 'data' dictionary below with your experimental results.
    """
    
    # ===== ENTER YOUR RESULTS HERE =====
    # Format: {num_nodes: throughput_in_ktps}
    data = {
        4: 133.5,   # Replace with your 4-node result
        5: 155.6,   # Replace with your 5-node result
        6: 152.3,   # Replace with your 6-node result
        7: 0.0,     # Replace with your 7-node result
        8: 0.0,     # Replace with your 8-node result
        9: 0.0,     # Replace with your 9-node result
    }
    # ====================================
    
    # Filter out zero values (not yet measured)
    measured_data = {k: v for k, v in data.items() if v > 0}
    
    if not measured_data:
        print("Error: No data to plot! Please update the 'data' dictionary with your results.")
        return
    
    # Extract nodes and throughputs
    nodes = sorted(measured_data.keys())
    throughputs = [measured_data[n] for n in nodes]
    
    # Create figure with higher quality
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Plot the data
    line = ax.plot(nodes, throughputs, marker='o', linewidth=3, markersize=12, 
                   color='#2E86AB', label='PirateShip (Leader-based)', 
                   markeredgewidth=2, markeredgecolor='white')
    
    # Add value labels on each point
    for x, y in zip(nodes, throughputs):
        ax.annotate(f'{y:.1f} ktps', 
                   xy=(x, y), 
                   xytext=(0, 12),
                   textcoords='offset points',
                   ha='center',
                   fontsize=11,
                   fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='white', 
                           edgecolor='#2E86AB', alpha=0.8))
    
    # Calculate and show trend
    if len(nodes) >= 2:
        # Show scaling efficiency
        base_tput = throughputs[0]
        base_nodes = nodes[0]
        
        # Ideal linear scaling line (dashed)
        ideal_nodes = np.array(nodes)
        ideal_tput = base_tput * (ideal_nodes / base_nodes)
        ax.plot(ideal_nodes, ideal_tput, '--', linewidth=2, 
               color='gray', alpha=0.5, label='Ideal Linear Scaling')
    
    # Formatting
    ax.set_xlabel('Number of Consensus Nodes', fontsize=15, fontweight='bold')
    ax.set_ylabel('Throughput (ktps)', fontsize=15, fontweight='bold')
    ax.set_title('Node Scaling Experiment: Leader Bottleneck Analysis', 
                fontsize=17, fontweight='bold', pad=20)
    
    # Grid styling
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.8)
    ax.set_axisbelow(True)
    
    # Set x-axis ticks
    ax.set_xticks(nodes)
    ax.set_xlim(nodes[0] - 0.3, nodes[-1] + 0.3)
    
    # Set y-axis limits with some padding
    y_min = min(throughputs)
    y_max = max(throughputs)
    y_range = y_max - y_min
    ax.set_ylim(max(0, y_min - y_range * 0.15), y_max + y_range * 0.15)
    
    # Legend
    ax.legend(fontsize=12, loc='best', framealpha=0.9)
    
    # Tight layout
    plt.tight_layout()
    
    # Save figure
    output_file = 'node_scaling_throughput.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Plot saved to: {output_file}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("NODE SCALING SUMMARY")
    print("="*60)
    print(f"{'Nodes':<8} {'Throughput':<15} {'vs Previous':<20} {'Efficiency':<15}")
    print("-"*60)
    
    prev_tput = None
    for i, (n, tput) in enumerate(zip(nodes, throughputs)):
        # Calculate change vs previous
        if prev_tput is None:
            change = "-"
        else:
            pct_change = ((tput - prev_tput) / prev_tput) * 100
            change = f"{pct_change:+.1f}%"
        
        # Calculate efficiency vs base (4 nodes)
        if i == 0:
            efficiency = "100.0% (baseline)"
        else:
            ideal_tput = throughputs[0] * (n / nodes[0])
            actual_efficiency = (tput / ideal_tput) * 100
            efficiency = f"{actual_efficiency:.1f}%"
        
        print(f"{n:<8} {tput:.1f} ktps{'':<6} {change:<20} {efficiency:<15}")
        prev_tput = tput
    
    print("="*60)
    
    # Bottleneck analysis
    if len(nodes) >= 3:
        # Check if throughput plateaus (increase < 5% from previous)
        plateau_node = None
        for i in range(1, len(throughputs)):
            pct_increase = ((throughputs[i] - throughputs[i-1]) / throughputs[i-1]) * 100
            if pct_increase < 5:
                plateau_node = nodes[i]
                break
        
        if plateau_node:
            print(f"\n⚠️  BOTTLENECK DETECTED at {plateau_node} nodes")
            print(f"   Throughput increase < 5% - leader saturation likely")
        else:
            print(f"\n✓ No clear bottleneck detected yet")
            print(f"  Consider testing with more nodes")
    
    print("\n")
    plt.show()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("PirateShip Node Scaling Experiment Plotter")
    print("="*60)
    print("\nEdit the 'data' dictionary in this script with your results,")
    print("then run again to generate the updated plot.\n")
    
    plot_node_scaling_results()
