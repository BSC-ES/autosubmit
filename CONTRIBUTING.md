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

For the Slurm tests (pytest marker `slurm`) you will need the Slurm container
built and running locally. We run it as a service in the CI pipeline, see the
file `.github/workflows/ci.yaml` for details -- there is a Docker service initialized
for the `test-slurm` job.

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
