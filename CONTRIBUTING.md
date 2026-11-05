# Contributing to Autosubmit

Autosubmit is an open source workflow manager developed at the Barcelona
Supercomputing Center. Contributions of all sizes are welcome, from a typo fix
in the documentation to a new platform or a new feature. You do not need to be
an HPC or Slurm expert to help.

- **Documentation:** https://autosubmit.readthedocs.io/
- **GitHub:** https://github.com/BSC-ES/autosubmit
- **Autosubmit support:** support-autosubmit@bsc.es

The `master` branch contains the code for the next release and may not match the
published documentation. Stable versions are published on PyPI. Other branches
are development branches and may require resolving dependencies manually. Ask
on the support address or open an issue if you need help working with a
non-release branch.

## Where to start

| I want to... | Go to |
| --- | --- |
| Report a bug | Open an issue with the [bug report template](.github/ISSUE_TEMPLATE/bug.md) |
| Propose a feature | Open an issue with the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) |
| Set up my environment | [Development environment](#development-environment) |
| Run the tests | [Running the tests](#running-the-tests) |
| Try Autosubmit manually with Slurm or Postgres | [Manual test environments](#manual-test-environments) |
| Send a change | [Submitting changes](#submitting-changes) |
| Use a local CI job | [Extras](#extras) |

## Contents

- [Before you start](#before-you-start)
- [Development environment](#development-environment)
- [Running the tests](#running-the-tests)
- [Manual test environments](#manual-test-environments)
- [Code quality](#code-quality)
- [Documentation](#documentation)
- [Submitting changes](#submitting-changes)
- [Getting help](#getting-help)
- [Extras](#extras)

## Before you start

Open an issue before working on a non-trivial change. It lets maintainers and
other contributors tell you whether the change fits the project, whether
someone is already working on it, and how it should be tested.

1. Fork https://github.com/BSC-ES/autosubmit.
2. Clone your fork, add the upstream remote and create a branch from `master`:
   ```bash
   git clone git@github.com:<your-user>/autosubmit.git
   cd autosubmit
   git remote add upstream https://github.com/BSC-ES/autosubmit.git
   git switch -c <issue-number>-<short-description>
   ```
3. Keep the branch focused on one concern. If the change needs a refactor that
   is not strictly required, send the refactor separately.

## Development environment

### Prerequisites

| Requirement | Notes |
| --- | --- |
| Python 3.10, 3.11 or 3.12 | `pyproject.toml` requires `>=3.10,<3.13` |
| Git | Needed by the test suite and by some experiments |
| Docker (optional) | Needed for `docker`, `slurm` and `postgres` tests and for the manual containers |
| Graphviz, rsync, curl (optional) | Used by some integration and regression tests |
| Subversion, Xvfb (optional) | Needed by the SVN and X11 integration tests. CI installs both on Ubuntu |
| R (optional) | Needed by the R tests. The tests skip themselves when `Rscript` is missing |
| sqlite3 (optional) | Used by CI to combine coverage files |

### Install from source

Always work inside a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip packaging
pip install -e ".[tests]"
```

Choose the extra group that matches your task, or install everything:

| Extra | Contents |
| --- | --- |
| `.[tests]` | pytest, pytest-xdist, pytest-mock, testcontainers and other test tools |
| `.[lint]` | ruff, pinned to the version used by CI |
| `.[mypy]` | mypy and the type stubs for the dependencies |
| `.[docs]` | Sphinx and the Read the Docs theme |
| `.[postgres]` | PostgreSQL drivers |
| `.[benchmark]` | pandas, matplotlib and tabulate |
| `.[all]` | All of the above. The test jobs install this group, the lint and mypy jobs install their own |

You can also install Autosubmit from GitHub without cloning:

```bash
pip install "git+https://github.com/BSC-ES/autosubmit.git@master"
```

### Initialize Autosubmit

`autosubmit expid`, `create`, `run` and the other commands need a configuration
file and a database. Create both once per user:

```bash
autosubmit configure
autosubmit install
```

`autosubmit configure` writes `$HOME/.autosubmitrc` and prompts on the command
line. Use `autosubmit configure -h` to see the available options. The pytest
suite does not need this step, because its fixtures create their own
configuration and database.

### Configure Git for the tests

Some integration tests create temporary Git repositories and commit to them. Set
an identity for the test suite, as CI does:

```bash
git config --global user.email "test@bsc.es"
git config --global user.name "Autosubmit tests"
```

The tests create repositories outside this checkout, so a repository-local
identity does not apply. Either set the global values above, or export
`GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME` and
`GIT_COMMITTER_EMAIL` in the shell that runs pytest.

## Running the tests

`pytest` reads `pytest.ini`. By default it runs the unit tests and doctests and
skips the tests marked `docker`, `postgres`, `slurm`, `local`, `asprofile`,
`profile`, `profilelong` and `benchmark`.

| Command | What it runs |
| --- | --- |
| `pytest` | Unit tests and doctests, with the default exclusions |
| `pytest test/integration` | Integration tests, with the default exclusions |
| `pytest test/regression` | Regression tests, with the default exclusions |
| `pytest test/integration -m docker` | Tests that need Docker |
| `pytest test/integration -m slurm` | Tests that need a Slurm container |
| `pytest test/integration -m git` | Tests that need a Git server |
| `pytest test/integration -m postgres` | Tests that need PostgreSQL |

For a quick local run that avoids the slow profiler and benchmark tests:

```bash
pytest -m "not asprofile and not profile and not profilelong and not benchmark"
pytest test/integration -m "not asprofile and not profile and not profilelong and not benchmark"
pytest test/regression -m "not asprofile and not profile and not profilelong and not benchmark"
```

These commands are an approximation, not a copy of CI. The CI jobs run:

```bash
pytest test/unit -m "not postgres and not profile and not profilelong"
pytest test/integration -m "not postgres and not slurm and not profile and not profilelong and not benchmark"
pytest test/integration -m "postgres and not slurm and not profile and not profilelong and not benchmark"
pytest test/integration -m "slurm and not profile and not profilelong"
pytest test/regression -m ""
```

The regression job disables all the marker filters, so it also runs tests that
the quick local run skips.

The default `-m` expression in `pytest.ini` still applies when you point pytest
to another directory. Pass `-m` again to override it.

Tests run in parallel with pytest-xdist, as `pytest.ini` already sets `-n auto`.

Avoid `pytest -m ""` as a default. It also runs the `docker`, `postgres`,
`slurm`, `local`, `profile` and `benchmark` tests, which need containers, the
`.[benchmark]` extra and a lot of time.

### Docker

The Docker tests use Testcontainers, which talks to the Docker daemon through
the Docker socket. Run Docker without `sudo` by adding your user to the
`docker` group, or grant access to the socket:

```bash
sudo setfacl --modify user:$USER:rw /var/run/docker.sock

# To undo it
sudo setfacl --remove user:$USER /var/run/docker.sock
```

The test fixtures start the containers they need, so you do not have to start
them by hand. The tests marked `slurm` use
[testcontainers-python](https://testcontainers-python.readthedocs.io/) to start
`giovtorres/slurm-docker:25.11.2-v0.1.7`, one container per test function that
uses the fixture. See https://github.com/giovtorres/slurm-docker for the image
and its configuration.

## Manual test environments

These instructions are only for testing by hand, for example when you want to
create an experiment and run it against a container that stays alive while you
work. The automated tests manage their own containers, see
[Running the tests](#running-the-tests).

### Slurm

This section uses the `autosubmit/slurm-openssh-container` image. The source is
at https://github.com/BSC-ES/slurm-openssh-container and the published tags are
at https://hub.docker.com/r/autosubmit/slurm-openssh-container. It is the image
used by older Autosubmit instructions and it is not the image that pytest
starts. The image ships a root SSH private key that lets you connect from the
host.

Pull and start it:

```bash
docker pull autosubmit/slurm-openssh-container:25-05-0-1
```

```bash
docker run --rm -it --cgroupns=host --privileged \
  --volume /sys/fs/cgroup:/sys/fs/cgroup:rw \
  --hostname slurmctld --name slurm-container \
  -p 2222:2222 autosubmit/slurm-openssh-container:25-05-0-1
```

In another terminal, copy the private key to the host and protect it. The image
names it `container_root_pubkey`, and the SSH client uses it as the identity
file:

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
docker cp slurm-container:/root/.ssh/container_root_pubkey "$HOME/.ssh/container_root_pubkey"
chmod 600 "$HOME/.ssh/container_root_pubkey"
```

If `docker cp` fails, check that the container is running and that its name is
`slurm-container` before continuing.

Add the host alias used by the experiment configuration:

```bash
cat <<- EOF >> $HOME/.ssh/config
Host localDocker
    HostName localhost
    User root
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    IdentityFile $HOME/.ssh/container_root_pubkey
    Port 2222
    ForwardX11 yes
EOF
```

Wait until Slurm is ready. The container takes a few seconds to start, and the
first connections may fail. Retry until `sinfo` lists the `normal` partition:

```bash
ssh localDocker "sinfo"
```

The image already creates `/tmp/scratch/group/root`, which the platform
definition below uses.

Now make Autosubmit use that host. Initialize Autosubmit first if you have not
done it yet (see [Initialize Autosubmit](#initialize-autosubmit)), then create
an experiment:

```bash
autosubmit expid -H TEST_SLURM -d "Manual Slurm test"
# The command prints the expid, for example a000, and the experiment folder.
cd <experiment-folder>
ls conf/platforms_<expid>.yml
```

Add a `TEST_SLURM` entry under `PLATFORMS`, keeping the rest of the file:

```yaml
PLATFORMS:
  # ... the platforms generated by expid ...

  TEST_SLURM:
    TYPE: slurm
    HOST: localDocker
    USER: root
    PROJECT: group
    QUEUE: normal
    SCRATCH_DIR: /tmp/scratch/
    TEMP_DIR: ''
    ADD_PROJECT_TO_HOST: false
    MAX_WALLCLOCK: '48:00'
    MAX_PROCESSORS: 2
    PROCESSORS_PER_NODE: 2
```

The values above match the image. Check them with `ssh localDocker "sinfo"` and
`ssh localDocker "scontrol show node"` if you use a different tag, and adjust
`QUEUE`, `PROJECT`, `SCRATCH_DIR` and the resources.

Create and run the experiment:

```bash
autosubmit create <expid>
autosubmit run <expid>
```

`HOST` is the alias from `~/.ssh/config`, not a hostname. This is what connects
Autosubmit to the container. If you created the experiment with `-min`, the
same `PLATFORMS` block goes in `conf/minimal.yml` or in the file it points to.

#### Cleanup

The container runs with `--rm`, so stopping it removes it:

```bash
docker stop slurm-container
```

Remove the `Host localDocker` block from `$HOME/.ssh/config` and delete
`$HOME/.ssh/container_root_pubkey` if you no longer need them. The experiments
stay in the Autosubmit directory and can be deleted with `autosubmit delete`.

### PostgreSQL

The repository includes a Docker Compose environment with Autosubmit and
PostgreSQL:

```bash
cd docker/postgres
./run.sh
```

It exposes PostgreSQL on `localhost:5432`, or on the port set with
`POSTGRES_HOST_PORT=5433 ./run.sh`. See
[docker/postgres/README.md](docker/postgres/README.md) for the full setup,
`psql` access and how to stop the environment.

## Code quality

These checks are part of the pull request workflow.

### Ruff

Ruff is the linter. Run it on every Python file you change, including new
untracked files:

```bash
ruff check path/to/changed_file.py
```

### mypy

CI type-checks these packages. Install the type stubs first:

```bash
pip install -e ".[mypy]"
mypy --install-types --non-interactive
```

```bash
mypy \
  --follow-imports=skip \
  --ignore-missing-imports \
  --check-untyped-defs \
  autosubmit/helpers \
  autosubmit/metrics \
  autosubmit/notifications \
  autosubmit/profiler \
  autosubmit/provenance \
  autosubmit/statistics
```

mypy also runs on the changed files as an advisory check. Fix what it reports
in the code you touched, and never silence a warning with a blanket ignore.

### Tests

Every bug fix needs a regression test that fails before the fix and passes
after it. Do not weaken, skip or delete an existing assertion to make a change
pass.

### Style

- Code must be readable by a human first. Use clear names, small functions and
  flat control flow.
- Use an early return for guard and sentinel cases. Do not write an `else`
  after a branch that returns, raises or continues.
- Types live in the signature. Docstrings use Sphinx `:param name:` and
  `:return:`, without `:type:` or `:rtype:`.
- Use `pathlib.Path` for filesystem paths.
- Database code uses SQLAlchemy idioms, not raw SQLite.
- Keep `try` blocks narrow. Broad handlers are acceptable only at process
  entry points.
- No comments unless they explain something the code cannot.

## Documentation

The documentation lives in `docs/` and is built with Sphinx. Warnings are
treated as errors, so a warning breaks the build.

```bash
make html -C ./docs/
```

CI builds the XML documentation and its coverage:

```bash
make xml_coverage -C ./docs/
```

This target runs `coverage`, which comes from `.[tests]` or `.[all]`, not from
`.[docs]`. Install `.[tests,docs]` or `.[all]` to use it. `make html` does not
need `coverage`.

If you change docstrings, CLI help or examples, build the documentation before
opening the pull request.

## Submitting changes

Before opening the pull request:

1. Run `git fetch upstream` and rebase on `upstream/master`. Keep the changes
   grouped by concern.
2. Run the tests that cover the modules you touched, plus the ruff and mypy
   checks above.
3. Add an entry to `CHANGELOG.md` only if the change affects users.
4. Update the documentation if you changed behavior, configuration or CLI help.
5. Update `pyproject.toml` if you added, removed or changed a dependency.
6. Fill in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md).

Link the issue in the pull request description, for example `Closes #1234`.
Changes that do not affect users, such as internal refactors or documentation
fixes for contributors, do not need a changelog entry.

GitHub Actions runs the unit, integration, regression, Slurm, documentation and
lint jobs on every pull request. A red blocking job means the change is not
ready to merge.

## Getting help

- Read the [Autosubmit documentation](https://autosubmit.readthedocs.io/).
- Search the [existing issues](https://github.com/BSC-ES/autosubmit/issues).
- Write to support-autosubmit@bsc.es.

## Extras

### Run GitHub Actions locally with `act`

Prerequisites: `docker`, [`act`](https://github.com/nektos/act) and a GitHub
token.

From the root of the repository, create an `event.json` file. This example
triggers the `metrics` job from a pull request comment:

```json
{
  "comment": { "body": "/metrics" },
  "issue": { "number": %pr_number%, "pull_request": { "url": "https://api.github.com/repos/BSC-ES/autosubmit/pulls/%pr_number%" } },
  "repository": { "full_name": "BSC-ES/autosubmit", "name": "autosubmit", "owner": { "login": "BSC-ES" } },
  "sender": { "login": "%yourusername%" }
}
```

Replace `%pr_number%` and `%yourusername%` with your pull request number and
GitHub username. You can create an `event.json` for other events, such as
`push`.

Run the `metrics` job:

```bash
act -j metrics -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -e event.json -s GITHUB_TOKEN="$GITHUB_TOKEN" --artifact-server-path /tmp/artifacts
```

Replace `metrics` with the name of the job you want to run.

To debug a job, enter its container:

```bash
act --reuse -j metrics -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -e event.json -s GITHUB_TOKEN="$GITHUB_TOKEN" --artifact-server-path /tmp/artifacts
docker exec -it <container_id> /bin/bash
cd /home/runner/work/<repo>/<repo>
```

### Random ports in tests

Some tests need a free TCP port. Instead of using a fixed range or a random
number, the test suite creates a socket without an address, reads the free port
assigned by the system, closes the socket and uses that port. This makes
collisions much less likely. See `test/integration/test_utils/networking.py`.

### VS Code Test Explorer

To discover, run and debug the test suites from the native VS Code Test
Explorer panel, add this configuration to `.vscode/settings.json`:

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
