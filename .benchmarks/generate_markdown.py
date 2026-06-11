from argparse import ArgumentParser
from pathlib import Path
from benchmark_utils import load_data
from importlib.metadata import version
parser = ArgumentParser(description="Generate performance comparison plots for Autosubmit.")
parser.add_argument("--version", type=str, required=False, help="Autosubmit version string for naming the summary file.")
args = parser.parse_args()
if not args.version:
    as_version = version("autosubmit")
else:
    as_version = args.version  # from workflow
markdown_path = Path(__file__).parent / "artifacts" / f"summary_{as_version}.md"

if markdown_path.exists():
    markdown_path.unlink()

datasets = load_data()
for i, data in enumerate(datasets):
    version_label = f"Version {as_version}"
    with open(markdown_path, "a") as md_file:
        md_file.write(f"# Autosubmit Performance Metrics - {version_label}\n\n")
        md_file.write(data.to_markdown(index=False) + "\n\n")
    print(f"Saved performance comparison markdown to {markdown_path}")
