#!/usr/bin/env python3
"""
Quick script to plot leader scaling results from existing logs.
Bypasses the grouping bug by manually parsing and grouping experiments.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from results import Result, Stats
from experiments import Experiment
import pickle
from pathlib import Path
import re
from dateutil.parser import isoparse
import datetime
import numpy as np
import matplotlib.pyplot as plt

# Import parsing functions
from results import process_tput, process_latencies, node_rgx, client_rgx

def parse_experiment_logs(exp_dir, duration=60, ramp_up=10, ramp_down=10):
    """Parse logs from a single experiment directory."""
    log_dir = exp_dir / "logs" / "0"
    
    if not log_dir.exists():
        return None
    
    # Parse node logs
    node_log_files = sorted([f for f in log_dir.glob("node*.log")])
    client_log_files = list(log_dir.glob("client*.log"))
    
    if not node_log_files:
        return None
    
    tputs = []
    tputs_unbatched = []
    latencies = []
    
    # Parse node logs
    points = []
    read_points = [[] for _ in node_log_files]
    
    with open(node_log_files[0], "r") as f:
        for line in f.readlines():
            captures = node_rgx.findall(line)
            if len(captures) == 1:
                points.append(captures[0])
    
    for node_num, node_log in enumerate(node_log_files):
        _rp = []
        with open(node_log, "r") as f:
            for line in f.readlines():
                captures = re.compile(r"\[INFO\]\[.*\]\[(.*)\] Total unlogged txs: ([0-9]+)").findall(line)
                if len(captures) == 1:
                    _rp.append(captures[0])
        read_points[node_num] = _rp
    
    try:
        _points = process_tput(points, duration, ramp_up, ramp_down, tputs, tputs_unbatched, byz=False, read_points=read_points)
    except:
        _points = []
    
    # Parse client logs
    client_points = []
    for log_file in client_log_files:
        try:
            with open(log_file, "r") as f:
                for line in f.readlines():
                    captures = client_rgx.findall(line)
                    if len(captures) == 1:
                        client_points.append(captures[0])
        except:
            pass
    
    try:
        process_latencies(client_points, duration, ramp_up, ramp_down, latencies, byz=False)
    except:
        pass
    
    if len(latencies) == 0:
        return None
    
    # Extract num_nodes and num_clients from experiment
    try:
        with open(exp_dir / "experiment.pkl", "rb") as f:
            exp = pickle.load(f)
            num_nodes = exp.num_nodes
            num_clients = exp.num_clients
    except:
        # Fallback: extract from directory name
        name = exp_dir.parent.name
        if "nodes_4" in name:
            num_nodes = 4
        elif "nodes_5" in name:
            num_nodes = 5
        elif "nodes_6" in name:
            num_nodes = 6
        elif "nodes_7" in name:
            num_nodes = 7
        elif "nodes_8" in name:
            num_nodes = 8
        elif "nodes_9" in name:
            num_nodes = 9
        else:
            num_nodes = 4
        num_clients = 600  # Default
    
    latency_prob_dist = np.array(latencies)
    latency_prob_dist.sort()
    p = 1. * np.arange(len(latency_prob_dist)) / (len(latency_prob_dist) - 1)
    
    mean_tput = np.mean(tputs) / 1000.0 if tputs else 0
    stdev_tput = np.std(tputs) / 1000.0 if tputs else 0
    mean_latency = np.mean(latencies) / 1000.0
    median_latency = np.median(latencies) / 1000.0
    
    return Stats(
        num_nodes=num_nodes,
        num_clients=num_clients,
        mean_tput=mean_tput,
        stdev_tput=stdev_tput,
        mean_tput_unbatched=np.mean(tputs_unbatched) / 1000.0 if tputs_unbatched else 0,
        stdev_tput_unbatched=np.std(tputs_unbatched) / 1000.0 if tputs_unbatched else 0,
        latency_prob_dist=latency_prob_dist,
        mean_latency=mean_latency,
        median_latency=median_latency,
        p25_latency=np.percentile(latencies, 25) / 1000.0,
        p75_latency=np.percentile(latencies, 75) / 1000.0,
        p99_latency=np.percentile(latencies, 99) / 1000.0,
        max_latency=np.max(latencies) / 1000.0,
        min_latency=np.min(latencies) / 1000.0,
        stdev_latency=np.std(latencies) / 1000.0
    )

def main():
    base_dir = Path("cloudlab_leader_scaling/2025-12-08T22-41-22.939390+00-00/experiments")
    
    if not base_dir.exists():
        print(f"Error: {base_dir} not found")
        return
    
    # Group experiments
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
    
    # Sort by num_nodes
    pirateship_stats.sort(key=lambda x: x.num_nodes)
    signed_raft_stats.sort(key=lambda x: x.num_nodes)
    
    print(f"PirateShip: {len(pirateship_stats)} experiments")
    print(f"Signed Raft: {len(signed_raft_stats)} experiments")
    
    # Create plot
    plot_dict = {
        "PirateShip": pirateship_stats,
        "Signed Raft": signed_raft_stats
    }
    
    xlabels = ["4", "5", "6", "7", "8", "9"]
    
    # Create bar graph with better styling
    from results import Result
    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
    from collections import OrderedDict
    
    # Create output directory
    output_dir = base_dir.parent.parent / "results" / "leader_node_scaling"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    plot_dict = OrderedDict(plot_dict)
    plot_matrix = np.zeros((len(xlabels), len(plot_dict)))
    stdev_matrix = np.zeros((len(xlabels), len(plot_dict)))
    max_tput = 0
    plot_dict_items = list(plot_dict.items())
    
    for i, xlabel in enumerate(xlabels):
        for j, (legend, stat_list) in enumerate(plot_dict_items):
            for k, stat in enumerate(stat_list):
                if i == k:
                    plot_matrix[i, j] = int(stat.mean_tput)
                    stdev_matrix[i, j] = int(stat.stdev_tput)
                    if stat.mean_tput > max_tput:
                        max_tput = stat.mean_tput
    
    ylim = max_tput * 1.1
    bar_width = 0.6
    gap_between_bars = 0.15
    block_size = 2 * gap_between_bars + len(plot_dict) * bar_width
    bar_start_pos = np.array([i * block_size for i in range(len(xlabels))])
    label_pos = [i * block_size + (len(plot_dict) // 2) * bar_width for i in range(len(xlabels))]
    
    # Better colors
    colors = ['#2E86AB', '#A23B72']  # Blue for PirateShip, Purple for Signed Raft
    
    # Use sans-serif font to avoid font warnings
    font = {"size": 16, "family": "sans-serif"}
    matplotlib.rc('font', **font)
    matplotlib.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'Liberation Sans']
    
    fig, ax = plt.subplots(figsize=(10, 6), layout="constrained")
    
    for i, (legend, stats) in enumerate(plot_dict_items):
        color = colors[i % len(colors)]
        rects = ax.bar(
            bar_start_pos + (gap_between_bars + i * bar_width),
            plot_matrix[:, i],
            width=bar_width,
            label=legend,
            color=color,
            alpha=0.8,
            edgecolor='black',
            linewidth=1.5,
            zorder=3,
            yerr=stdev_matrix[:, i],
            capsize=4,
            error_kw={'elinewidth': 2, 'capthick': 2}
        )
    
    ax.set_xticks(label_pos, xlabels)
    ax.set_ylim(0, ylim + 100)
    ax.set_ylabel("Throughput (k req/s)", fontsize=16, fontweight='bold')
    ax.set_xlabel("Number of Nodes", fontsize=16, fontweight='bold')
    ax.tick_params(axis='both', labelsize=13)
    ax.grid(True, alpha=0.3, linestyle='--', zorder=0, linewidth=0.8)
    ax.legend(loc="best", fontsize=13, framealpha=0.95, edgecolor='gray', fancybox=True)
    
    # Improve bar appearance
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(0.8)
    ax.spines['bottom'].set_linewidth(0.8)
    
    output_path = output_dir / "leader_node_scaling.pdf"
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    print(f"\nBar graph saved to: {output_path}")

if __name__ == "__main__":
    main()

