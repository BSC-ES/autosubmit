## Autosubmit contribution guide

**Documentation:** http://autosubmit.readthedocs.io/en/latest/

**GitHub:** https://github.com/bsc-es/autosubmit

**Mailing list:** autosubmit@bsc.es

The production branch generally reflects the Autosubmit release on PyPI,
and is considered stable: it should work 'out of the box' for the supported
backends. For a list of supported backends, please refer to the documentation.

The `master` branch (and any other branches than production, for that matter)
may not correspond to the published documentation, and specifically may have
dependencies which need to be resolved manually. Please contact us over the
mailing list if you need advice on the usage of any non-production branch.

## Building

First, update your building tools and libraries:

```bash
$ pip install -U pip packaging
```

Then choose how you want to install Autosubmit (choose one):

```bash
$ pip install -U -e .         # for editable/development mode
$ pip install -U -e .[all]    # for editable/development mode, including all dependencies
$ pip install -U -e .[tests]  # or include only the test dependencies
$ pip install .               # to install from source, without updating dependencies
$ pip install autosubmit      # to install from PyPI
```

Another way, less conventional though, to install Autosubmit is to use
GitHub directly from `pip`:

```bash
# Use a branch
$ pip install git+https://github.com/bsc-es/autosubmit.git@history_db_lint_fix
# Use a Git commit
$ pip install -U git+https://github.com/bsc-es/autosubmit.git@69a506f12c471b49fd021b3448b7d5bc215f1183
```

## Run the tests

In order to run the tests, you will require a compatible version of Python,
preferably a virtual environment (with Mamba, Conda, `venv`, etc.), and the
test dependencies installed (for the `tests` optional group, see previous
section "Building").

It recommended to always run the tests locally when preparing a contribution
to the project. This can be done with the command below from the root directory
of your working copy of the code:

```bash
$ pytest
```

The `pytest` command will read the configuration settings defined in the
`pytest.ini` file. You can change it if needed, e.g. `pytest -m "not some-marker"`.

The project also includes integration tests, that can be executed pointing
`pytest` to the directory with those tests (by default, `pytest.ini` points to
`test/unit` directory):

```bash
$ pytest test/integration
```

This will use the same settings from `pytest.ini`, but will run the tests in
the directory you specified (`test/integration`).

GitHub Actions run extra tests that require Docker. If you would like to run
those tests locally too, then you must have Docker installed in your system,
preferably being able to run `docker` without `sudo` (i.e. adding your `$USER`
to the `docker` group), and you also need access to the Docker socket.

> NOTE: the access to the Docker socket is required as the test library
>       Testcontainers will query the Docker API to instantiate/edit/destroy
>       containers on-the-fly, and without that access the tests will fail.

```bash
# Grant permission to your `$USER` to read and write to the Docker socket
$ sudo setfacl --modify user:$USER:rw /var/run/docker.sock
# To undo it
# $ sudo setfacl --remove user:$USER /var/run/docker.sock
```

You can follow the steps specified at the [Giovtorres Slurm Git Repository](https://github.com/giovtorres/slurm-docker-cluster) if you would like to have a Slurm container
locally for the tests marked with `slurm`. This step is optional as each test
launches the container in an isolated manner by using [testcontainers-python](https://testcontainers-python.readthedocs.io/en/latest/) library with pytest.

Then you can run all the tests, with

```bash
$ pytest -m ""
```

or just the tests that require Docker,

```bash
$ pytest -m 'docker'
```

or just the tests that require Slurm:

```bash
$ pytest -m 'slurm'
```

### Running the tests on VS Code

If you want to discover, run, and debug your test suites directly inside the native Visual Studio Code Test Explorer panel, you must configure `pytest` arguments to find the decoupled test directories.

Create or append the following configuration to your local workspace settings file (`.vscode/settings.json`):

```json
{
    "python.testing.pytestArgs": [
        "test/unit",
        "test/integration",
        "test/regression",
        "--override-ini=addopts=--strict-markers --doctest-modules --durations=5"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

## Random ports

Some tests require random ports. To acquire a free random port, we rely
on a Linux feature where we create a socket without specifying address
or port number. The socket created has a random free port in the system.
We close the socket and use that port for our next test. Chances or the
port being used by multiple tests is smaller than using random or ranges.

See `test/integration/test_utils/networking.py` for more.

## Postgres Database. 

In case you want to do some manual testing with the Postgres backend,
you can use the following command to start a Postgres container:

```bash
$ sudo setfacl --modify user:$USER:rw /var/run/docker.sock
```

```bash
$ docker run -d --name some-postgres \
    -e POSTGRES_PASSWORD=mypwd \
    -e POSTGRES_USER=postgres \
    -p 5432:5432 \
    postgres 
```

## Benchmarks

Autosubmit includes a performance benchmark suite to keep track of the cost of
`create`, `run`, `recovery` and `setstatus` for experiments of increasing size.
The tests live in `test/integration/commands/test_performance.py` and are marked
with `profile` (quick) and `profilelong`. They use [pytest-benchmark](https://pytest-benchmark.readthedocs.io)
and the Autosubmit Profiler; the wall-clock time is measured by pytest-benchmark
and the profiler metrics (memory, DB sizes, file descriptors, ...) are stored in
each run's `extra_info`.

The results are stored as pytest-benchmark JSON runs under `.benchmarks/data`
and a comparison against the stored baseline is produced by
`.benchmarks/compare_results.py` using the thresholds in
`.benchmarks/thresholds.yml`. Both directories are git-ignored; the scripts are
tracked.

### How it is triggered

The performance check runs **before merge** so regressions are caught in time,
and the baseline is updated by **guarded promotion** (a run that regresses is
never promoted).

* **PR gate (required status check)**: the `metrics` workflow runs on every
  pull request. It is a cheap no-op unless the PR carries the `perf-benchmark`
  label, in which case it runs the full suite (`profilelong`) against the
  baseline and the merge button stays disabled until the benchmark completes.
  While the label is on, every push triggers a full run and the `benchmark`
  concurrency group serializes them, so remove the label once the check passes.
  Add the label to PRs that may affect runtime performance (requires write/triage
  access; if you cannot add labels, say so in the PR description so a maintainer
  can). See the PR template and the "Set up the merge gate" note below.
* **Manually**: a member of the `BSC-ES/autosubmit` team (or a user listed as
  `@username` in this repository's `.benchmarks/allowed-users.txt` on the default branch)
  comments on a PR:
  * `/metrics` — quick suite
  * `/metrics_full` — full suite
  * `/metrics_promote` — promote the last completed run as the new baseline. Run `/metrics` (or `/metrics_full`) first, then `/metrics_promote`
    to re-baseline that result. The run being promoted must match the current PR
    head: if you pushed after the last benchmark, re-run `/metrics` (or
    `/metrics_full`) first.


The PR results are posted as a comment comparing them against the baseline,
flagging regressions beyond the configured thresholds as warnings. Only the
plots are visible at first glance; the regressions and scenario tables are
collapsed in a `<details>` block. Two comparison plots are stored on the
`benchmark-reference` branch and linked from the comment (GitHub strips `data:`
image URIs, so plots are not embedded inline): one for the `run`
scenarios (which carry the profiler growth metrics) and one for
`create`/`recovery`/`setstatus`. The comment also links the run's artifacts for
direct download (raw benchmark data and the report with markdown, plots and
JSON). Only the latest plots are kept.

### The baselines

The baselines live on the `benchmark-reference` branch (`.benchmarks/reference`),
and every promotion is a commit, so its history is preserved.

Baselines are stored **per CPU model**:
`.benchmarks/reference/<cpu-slug>/` where the slug derives from the runner's
CPU (`machine_info.cpu.brand_raw`, e.g. `intel-r-xeon-r-platinum-8370c-cpu-2-80ghz`).
GitHub-hosted runners do not guarantee a fixed CPU, so a run is only compared against the baseline of the same CPU;
results across different CPUs are not comparable. Baselines fill lazily: the
first run on a given CPU establishes that CPU's baseline (reported as "no
baseline yet"), and later runs on the same CPU are compared against it.

To restore a previous baseline after a bad merge:

```bash
git fetch origin benchmark-reference
git checkout <previous-baseline-sha> -- .benchmarks/reference
git commit -m "Restore baseline before merge <merge-sha>"
git push origin HEAD:benchmark-reference
```

### Set up the merge gate

1. Create the `perf-benchmark` label (Settings > Labels).
2. In branch protection for the default branch, enable **Require status
   checks** and select **`performance-benchmark`** (the job name in
   `.github/workflows/metrics.yaml`).

### Running the benchmarks locally

Install the benchmark dependencies and run the suite (Docker and a Slurm
container are required for the `run`, `recovery` and `setstatus` scenarios; the
`create` scenarios can run without them):

```bash
$ pip install -e .[all]
$ pytest -m profile -n 0 --benchmark-save=mylabel \
    test/integration/commands/test_performance.py
```

> NOTE: run the suite with `-n 0`. pytest-benchmark cannot save runs under
> `pytest-xdist` parallelism.

To compare two runs (e.g. a local baseline against a newer run) and generate the
markdown report and plot:

```bash
$ python .benchmarks/compare_results.py \
    --current .benchmarks/data/current.json \
    --previous .benchmarks/data/baseline.json \
    --thresholds .benchmarks/thresholds.yml \
    --version "$(cat VERSION)" \
    --output-dir .benchmarks/artifacts
```

The report is written to `.benchmarks/artifacts/summary_<version>.md` and the
two grid plots (one per scenario group, `run` and
`create`/`recovery`/`setstatus`) to `summary_<version>_run.png` and
`summary_<version>_create_recovery_setstatus.png`. Cells are colored red/blue by
change direction (with a neutral dead zone for |delta| below
`plot.delta_tolerance`, configurable in `.benchmarks/thresholds.yml`) and
annotated with the current value; within each group rows are ordered from
fastest to slowest. Without a baseline, cells are neutral and only show the
values.

### Adding scenarios or metrics

New parametrizations of an existing command (`create`, `run`, `recovery`,
`setstatus`) are picked up automatically: they appear as a new row in the
corresponding plot, ordered by time within their group.

A new **test type** (e.g. a new command being benchmarked) or a new **metric**
needs a small change in `.benchmarks/compare_results.py`:

* **Test type**: add it to `_RUN_TEST_TYPES` (carries the profiler growth
  metrics) or `_OTHER_TEST_TYPES` (time/memory/DB metrics), or add a new plot
  entry in `render_heatmaps()`. If the new type should not carry the growth
  metrics (`FD GROWTH`, `MEM GROWTH`, `OBJ GROWTH`), also add it to
  `_NO_GROW_TEST_TYPES`.
* **Metric**: add it to `METRIC_COLUMNS` so `build_frame()` stores it (and it
  shows up in the markdown tables), then to the matching plot metric list
  (`_RUN_PLOT_METRICS` or `_OTHER_PLOT_METRICS`) so the plot renders it. The
  test must write it into `benchmark.extra_info` (see
  `_collect_profiler_metrics` in `test/integration/commands/test_performance.py`).

## Test GitHub Actions locally

Prerequisites: `docker`, `act` and a GitHub token.

Go to the root directory of the repository and configure one `event.json` file
with the content below ( for a PR ) :

Example to trigger the `metrics` job on a PR comment `/metrics`:

```json
{
  "comment": { "body": "/metrics" }, 
  "issue": { "number": %pr_number%, "pull_request": { "url": "https://api.github.com/repos/BSC-ES/autosubmit/pulls/%pr_number%" } },
  "repository": { "full_name": "BSC-ES/autosubmit", "name": "autosubmit", "owner": { "login": "BSC-ES" } },
  "sender": { "login": "%yourusername%" }
}
```
Note: replace `%pr_number%` and `%yourusername%` with your PR number and GitHub username.
Note: you can also create an `event.json` for other events, e.g., `push`.

Then you can run `act` with:

```bash
$ act -j metrics -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -e event.json -s GITHUB_TOKEN="$GITHUB_TOKEN" --artifact-server-path /tmp/artifacts
```
replace `metrics` with the name of the job you want to run (`authorize`,
`metrics`, `report`, `update-baseline`).

For debugging purposes, you can also enter the container where the job is
being executed with:

```bash
$ act --reuse -j metrics -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -e event.json -s GITHUB_TOKEN="$GITHUB_TOKEN" --artifact-server-path /tmp/artifacts
$ docker exec -it <container_id> /bin/bash
cd /home/runner/work/<repo>/<repo>
```
