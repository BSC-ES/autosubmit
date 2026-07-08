from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.patches as mpatches
from argparse import ArgumentParser
from benchmark_utils import load_data, get_last_version_names

parser = ArgumentParser(description="Generate performance comparison plots for Autosubmit.")
parser.add_argument("--plot", default=False, action="store_true", help="Display the plot after generation.")
parser.add_argument("--version", type=str, required=False,
                    help="Autosubmit version string for naming the summary file.")

args = parser.parse_args()
plot = args.plot


def plot_data(current_data: pd.DataFrame, previous_data: pd.DataFrame = None, show: bool = False) -> None:
    """Plot performance metrics comparison between current and previous data, one file per test type.

    :param current_data: DataFrame containing current version metrics.
    :type current_data: pd.DataFrame
    :param previous_data: DataFrame containing previous version metrics, or None.
    :type previous_data: Optional[pd.DataFrame]
    :param show: Whether to display the plot interactively.
    :type show: bool
    """
    metrics = [
        "Time Taken(Seconds)",
        "Memory consumption(MiB)",
        "Historical DB Disk Usage(MiB)",
        "Job list DB Usage",
        "Total Jobs",
        "Total Dependencies",
    ]

    for test_type in ["create", "run", "run_heavy", "recovery", "setstatus"]:
        if "run" in test_type:
            metrics.extend(["FD GROW",
                            "MEM GROW(MIB)",
                            "OBJ GROW", ])
        # access to grouped data for the current test type
        current_slice = current_data.get_group(test_type)
        if current_slice.empty:
            print(f"No data for test type '{test_type}', skipping.")
            continue
        if previous_data:
            previous_slice = previous_data.get_group(test_type)
            if previous_slice.empty:
                print(f"No previous data for test type '{test_type}', skipping previous version comparison.")
                previous_slice = None
        else:
            previous_slice = None

        fig, axes = plt.subplots(3, 3, figsize=(20, 15))
        fig.suptitle(
            f"Autosubmit Performance Metrics — {test_type} (Version: {as_version})",
            fontsize=16,
        )

        for ax, metric_col in zip(axes.flatten(), metrics):
            current_metrics = current_slice[metric_col].astype(float)
            ax.bar(current_slice["ID"], current_metrics, color="blue", alpha=0.6, label="Current Version")

            if previous_slice is not None:
                previous_metrics = previous_slice[metric_col].astype(float)
                ax.bar(previous_slice["ID"], previous_metrics, color="orange", alpha=0.2, label="Previous Version")

            ax.set_title(metric_col)
            ax.set_xlabel("ID")
            ax.set_ylabel(metric_col)
            ax.tick_params(axis="x", rotation=45)

            if previous_slice is not None:
                blue_patch = mpatches.Patch(color="blue", label="Current Version")
                orange_patch = mpatches.Patch(color="orange", label="Previous Version")
                ax.legend(handles=[blue_patch, orange_patch])

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt_path = Path(__file__).parent / "artifacts" / f"summary_{test_type}.png"
        plt.savefig(plt_path)
        print(f"Saved performance comparison plot to {plt_path}")
        if show:
            plt.show()
        plt.close(fig)


def autosubmit_version():
    """Reads the version number from the VERSION file."""
    with open(Path(__file__).parent.parent / "VERSION", "r") as file:
        content = file.read()
    return content.strip(" \n")


as_version = args.version if args.version else autosubmit_version()
version_names = get_last_version_names()

datasets = load_data(version_names)

if len(version_names) < 2:
    plot_data(datasets[version_names[0]], None, show=plot)
else:
    plot_data(datasets[version_names[0]], datasets[version_names[1]], show=plot)
