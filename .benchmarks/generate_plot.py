from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.patches as mpatches
from argparse import ArgumentParser
from benchmark_utils import load_data
from importlib.metadata import version

parser = ArgumentParser(description="Generate performance comparison plots for Autosubmit.")
parser.add_argument("--plot", default=False, action="store_true", help="Display the plot after generation.")
parser.add_argument("--version", type=str, required=False, help="Autosubmit version string for naming the summary file.")

args = parser.parse_args()
plot = args.plot


def plot_data(current_data: pd.DataFrame, previous_data: pd.DataFrame = None, show=False) -> plt.Figure:
    """Plot performance metrics comparison between current and previous data.
    1. Time Taken
    2. Memory consumption
    3. Disk Usage(Historical)
    4. Disk Usage(Joblist)
    :param current_data: DataFrame containing current version metrics.
    :param previous_data: DataFrame containing previous version metrics.
    :param show: Whether to display the plot.
    :return: Matplotlib Figure object.
    """
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f"Autosubmit Performance Metrics Comparison (Version: {as_version})", fontsize=16)

    metrics = [
        ("Time Taken", "Time Taken (seconds)"),
        ("Memory consumption", "Memory Consumption (MiB)"),
        ("Disk Usage(Historical)", "Disk Usage Historical (MiB)"),
        ("Disk Usage(Joblist)", "Disk Usage Joblist (MiB)")
    ]

    for ax, (metric_col, metric_label) in zip(axes.flatten(), metrics):
        current_metrics = current_data[metric_col].astype(float)
        ax.bar(current_data["ID"], current_metrics, color='blue', alpha=0.6, label='Current Version')

        if previous_data is not None:
            previous_metrics = previous_data[metric_col].astype(float)
            ax.bar(previous_data["ID"], previous_metrics, color='orange', alpha=0.2, label='Previous Version')

        ax.set_title(metric_label)
        ax.set_xlabel("Run ID")
        ax.set_ylabel(metric_label)
        ax.tick_params(axis='x', rotation=45)

        if previous_data is not None:
            blue_patch = mpatches.Patch(color='blue', label='Current Version')
            orange_patch = mpatches.Patch(color='orange', label='Previous Version')
            ax.legend(handles=[blue_patch, orange_patch])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt_path = Path(__file__).parent / "artifacts" / f"summary_{as_version}.png"
    plt.savefig(plt_path)
    print(f"Saved performance comparison plot to {plt_path}")
    if show:
        plt.show()


as_version = version("autosubmit")

datasets = load_data()

# Right now only bars for the last two versions
if len(datasets) < 2:
    plot_data(datasets[0], None, show=plot)
else:
    plot_data(datasets[0], datasets[1], show=plot)
