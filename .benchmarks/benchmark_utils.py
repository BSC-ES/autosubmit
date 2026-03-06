import pandas as pd
from pathlib import Path


def load_data(artifact_folder: str = None) -> list[pd.DataFrame]:
    """Load benchmark data from CSV files in the specified folder.
    data is expected to have the following columns:
    ID, Time Taken, Memory consumption, Disk Usage(Historical), Disk Usage(Joblist),
    Total Jobs, Total Dependencies
    File is expected to be in .benchmarks/artifacts/
    File names are expected to be in the format ref_metrics_{version}.csv
    :param artifact_folder: Path to the folder containing benchmark CSV files. If None, defaults to .benchmarks/artifacts/
    :rtype: list[pd.DataFrame]
    :return: List of DataFrames, each corresponding to a version's benchmark data.
    """
    if not artifact_folder:
        artifact_folder = Path(__file__).parent / "artifacts"
    else:
        artifact_folder = Path(artifact_folder)

    artifact_folder = sorted([f for f in artifact_folder.iterdir() if f.suffix == ".csv"], key=lambda x: x.name)
    print(f"Found artifact files: {[f.name for f in artifact_folder]}")
    data_by_version = []

    for artifact_file in artifact_folder:
        data = pd.read_csv(artifact_file)
        data = data.drop_duplicates(subset=["ID"], keep="last")
        data_by_version.append(data)

    return data_by_version


def inject_advanced_data(datasets):
    """Inject advanced benchmark data into the datasets.
    This is a placeholder function for future implementation.
    Advanced data may include metrics such as:
        1) Speed up
        2) Memory grow per iteration
        3) Disk usage grow per iteration
        4) ...
    """
    raise NotImplementedError("This function is a placeholder for future advanced data injection.")

##
