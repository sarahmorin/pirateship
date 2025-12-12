#!/usr/bin/env python3
"""Analyze throughput values from leader scaling experiments."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from quick_plot_leader_scaling import parse_experiment_logs
from pathlib import Path

def main():
    base_dir = Path("cloudlab_leader_scaling/2025-12-08T22-41-22.939390+00-00/experiments")
    
    if not base_dir.exists():
        print(f"Error: {base_dir} not found")
        return
    
    pirateship_stats = []
    signed_raft_stats = []
    
    for exp_name in sorted(base_dir.iterdir()):
        if not exp_name.is_dir():
            continue
        
        stats = parse_experiment_logs(exp_name / "0")
        if stats is None:
            print(f"Warning: Could not parse {exp_name}")
            continue
        
        if "pship" in exp_name.name:
            pirateship_stats.append(stats)
        elif "sraft" in exp_name.name:
            signed_raft_stats.append(stats)
    
    pirateship_stats.sort(key=lambda x: x.num_nodes)
    signed_raft_stats.sort(key=lambda x: x.num_nodes)
    
    print("="*70)
    print("THROUGHPUT ANALYSIS - Leader Scaling Experiments")
    print("="*70)
    
    print("\nPirateShip Throughput (k req/s):")
    print("-" * 70)
    print(f"{'Nodes':<8} {'Mean Tput':<12} {'Stdev':<10} {'Min':<10} {'Max':<10}")
    print("-" * 70)
    pship_tputs = []
    for s in pirateship_stats:
        pship_tputs.append(s.mean_tput)
        print(f"{s.num_nodes:<8} {s.mean_tput:<12.2f} {s.stdev_tput:<10.2f} {min(pship_tputs) if pship_tputs else 0:<10.2f} {max(pship_tputs) if pship_tputs else 0:<10.2f}")
    
    print(f"\nPirateShip Range: {min(pship_tputs):.2f} - {max(pship_tputs):.2f} k req/s")
    print(f"PirateShip Average: {sum(pship_tputs)/len(pship_tputs):.2f} k req/s")
    
    print("\n" + "="*70)
    print("\nSigned Raft Throughput (k req/s):")
    print("-" * 70)
    print(f"{'Nodes':<8} {'Mean Tput':<12} {'Stdev':<10} {'Min':<10} {'Max':<10}")
    print("-" * 70)
    sraft_tputs = []
    for s in signed_raft_stats:
        sraft_tputs.append(s.mean_tput)
        print(f"{s.num_nodes:<8} {s.mean_tput:<12.2f} {s.stdev_tput:<10.2f} {min(sraft_tputs) if sraft_tputs else 0:<10.2f} {max(sraft_tputs) if sraft_tputs else 0:<10.2f}")
    
    print(f"\nSigned Raft Range: {min(sraft_tputs):.2f} - {max(sraft_tputs):.2f} k req/s")
    print(f"Signed Raft Average: {sum(sraft_tputs)/len(sraft_tputs):.2f} k req/s")
    
    print("\n" + "="*70)
    print("\nCOMPARISON:")
    print("-" * 70)
    print(f"{'Nodes':<8} {'PirateShip':<15} {'Signed Raft':<15} {'Difference':<15} {'% Diff':<10}")
    print("-" * 70)
    for i in range(len(pship_tputs)):
        pship = pship_tputs[i]
        sraft = sraft_tputs[i]
        diff = pship - sraft
        pct_diff = (diff / sraft * 100) if sraft > 0 else 0
        nodes = pirateship_stats[i].num_nodes
        print(f"{nodes:<8} {pship:<15.2f} {sraft:<15.2f} {diff:<15.2f} {pct_diff:<10.2f}%")
    
    print("\n" + "="*70)
    print("\nSCALING ANALYSIS:")
    print("-" * 70)
    print("PirateShip scaling (relative to 4 nodes):")
    base_pship = pship_tputs[0]
    for i, s in enumerate(pirateship_stats):
        if i == 0:
            print(f"  {s.num_nodes} nodes: {s.mean_tput:.2f} k req/s (baseline)")
        else:
            ideal = base_pship * (s.num_nodes / 4.0)
            efficiency = (s.mean_tput / ideal) * 100 if ideal > 0 else 0
            print(f"  {s.num_nodes} nodes: {s.mean_tput:.2f} k req/s (ideal: {ideal:.2f}, efficiency: {efficiency:.1f}%)")
    
    print("\nSigned Raft scaling (relative to 4 nodes):")
    base_sraft = sraft_tputs[0]
    for i, s in enumerate(signed_raft_stats):
        if i == 0:
            print(f"  {s.num_nodes} nodes: {s.mean_tput:.2f} k req/s (baseline)")
        else:
            ideal = base_sraft * (s.num_nodes / 4.0)
            efficiency = (s.mean_tput / ideal) * 100 if ideal > 0 else 0
            print(f"  {s.num_nodes} nodes: {s.mean_tput:.2f} k req/s (ideal: {ideal:.2f}, efficiency: {efficiency:.1f}%)")

if __name__ == "__main__":
    main()





