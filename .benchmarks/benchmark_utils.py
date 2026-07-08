import pandas as pd
from pathlib import Path


def get_last_version_names(artifact_folder: str = None) -> list[str]:
    """Get the names of the last two versions from the artifact folder.
    File names are expected to be in the format ref_metrics_{version}.csv
    :param artifact_folder: Path to the folder containing benchmark CSV files. If None, defaults to .benchmarks/artifacts/
    :rtype: list[str]
    :return: List of version names, sorted by version number, with the last two versions included.
    """
    if not artifact_folder:
        artifact_folder = Path(__file__).parent / "artifacts"
    else:
        artifact_folder = Path(artifact_folder)

    artifact_files = sorted([f for f in artifact_folder.iterdir() if f.suffix == ".csv" and "iteration" not in f.name],
                            key=lambda x: x.name)
    print(f"Found artifact files: {[f.name for f in artifact_files]}")

    version_names = [f.stem.split("_")[-1] for f in artifact_files]
    if len(version_names) > 2:
        version_names = version_names[-2:]

    return version_names


def load_data(version_names, artifact_folder: str = None) -> dict[str, pd.DataFrame]:
    """Load benchmark data from CSV files in the specified folder.

    File is expected to be in .benchmarks/artifacts/

    :param version_names: List of version names to load data for. Only files with these version names will be loaded.
    :param artifact_folder: Path to the folder containing benchmark CSV files. If None, defaults to .benchmarks/artifacts/
    :rtype: list[pd.DataFrame]
    :return: List of DataFrames, each corresponding to a version's benchmark data.
    """
    if not artifact_folder:
        artifact_folder = Path(__file__).parent / "artifacts"
    else:
        artifact_folder = Path(artifact_folder)

    artifact_folder = sorted([f for f in artifact_folder.iterdir() if f.suffix == ".csv" and "iteration" not in f.name and f.stem.split("_")[-1] in version_names],
                             key=lambda x: x.name)
    print(f"Found artifact files: {[f.name for f in artifact_folder]}")
    data_by_version = {}

    if len(artifact_folder) > 2:
        artifact_folder = artifact_folder[-2:]
    for artifact_file in artifact_folder:
        print(f"Loading data from: {artifact_file.name}")
        versioned_data = pd.read_csv(artifact_file)
        version = artifact_file.stem.split("_")[-1]
        data_by_version[version] = versioned_data.groupby("test type")

    return data_by_version
