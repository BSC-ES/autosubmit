# Autosubmit Tier 1 & Tier 2 Performance and Storage Remediation

- **Author:** Vladimir Lapin
- **Status:** Approved
- **Date:** 2026-11-04

--------------------------------------------------------------------------------

## Overview

Autosubmit coordinates numerical modeling workflows on High-Performance Computing (HPC) platforms. Production workflows regularly exceed 10,000 jobs. At this scale, Autosubmit versions 4.17.2 and 4.2.0 experience command latency, CPU freezes, and log storage consumption exceeding 1 TB.

This design document specifies targeted optimizations that provide significant latency and storage reductions without architectural rewrites or breaking backwards compatibility. The focus is on validated bottlenecks that yield immediate performance gains and storage reductions while preserving existing workflow definitions.

## Problem Statement

On experiments with 10,000 or more jobs, operators encounter three main bottlenecks:

1. **Interactive CLI Latency**:
   - `Job.__init__` invokes `BasicConfig.read()` on every instance, parsing `.autosubmitrc` from disk 10,014 times during workflow creation. This redundant disk I/O consumes 6.62 seconds (73.7% of workflow creation time).
   - `autosubmit stats` triggers an in-loop `as_conf.reload()` for every job inside `Job.update_parameters()`, re-parsing configuration files from disk 10,013 times and locking a CPU core at 100% for 61.7 seconds.
   - `JobList.get_job_by_name()` performs a linear scan over an ephemeral list of all jobs on every lookup. This scan consumes 5 to 11 seconds of CPU time during each polling cycle.
   - `autosubmit monitor` uses recursive depth-first search for text tree generation, crashing with `RecursionError` on workflows with long sequential dependency chains (for example, >= 1,000 chunks).

2. **Storage Footprint and Log Inode Exhaustion**:
   - Workflows generate up to 1–2 TB of uncompressed text logs, exhausting HPC scratch storage quotas.
   - Remote compression defaults to disabled (`compress_remote_logs = False`), and log files are retained on both remote scratch and local filesystems.
   - The CLI log viewer (`autosubmit cat-log`) fails on compressed files, preventing users from inspecting compressed archives.
   - Default run logging operates at `DEBUG` verbosity on disk, creating 50 GB log files.

3. **Log Severity Scale Inversion**:
   - In `autosubmit/log/log.py`, `WARNING` (3000) is numerically lower than `INFO` (4000). Setting the log filter to `INFO` silences all system warnings.
   - In `autosubmit/scripts/autosubmit.py`, `--logconsole` defaults to `WARNING`. When severity levels are corrected, console logging will mute all `INFO` records unless the default is updated.

## Terminology

- **`as_conf`**: An instance of `AutosubmitConfig` managing experiment and platform YAML configurations.
- **`chunk`**: An individual, sequential execution slice in a climate simulation (for example, one month or one year of model integration).
- **`<expid>`**: The 4-character experiment identifier (for example, `a000` or `a002`).

## Goals

- **Eliminate Redundant Disk Parsing**: Cache `BasicConfig` in memory so `.autosubmitrc` is read once, reducing 10,000-job graph creation latency by ~6.6 seconds.
- **Fix CLI Command Freezes**: Decouple configuration reloading from job parameter updates, reducing `autosubmit stats` wall time from ~62 seconds to < 2 seconds.
- **Accelerate Graph Resolution**: Provide $O(1)$ lookups for `JobList.get_job_by_name()` using existing NetworkX graph node attributes, eliminating linear lookup scans.
- **Prevent Stack Overflow Crashes**: Refactor `_recursion_print` in `autosubmit monitor` from recursive traversal to an iterative worklist, supporting deep chunk chains (>= 1,000 chunks) without `RecursionError`.
- **Reduce Log Storage Footprint**: Reduce job log storage footprint by > 80% (saving ~834 GB per TB) by enabling remote gzip compression and making `autosubmit cat-log` decompress `.gz` files.
- **Reduce Daemon Log Churn**: Set the default file log level to `INFO` while retaining `DEBUG` via `-lf DEBUG`, cutting `_run.log` files from ~50 GB to < 100 MB.
- **Restore Warning and Console Visibility**: Re-align logger severity levels so that `WARNING` (4000) ranks above `INFO` (3000), and update `--logconsole` default to `INFO`.
- **Standard Python Invocation**: Provide `autosubmit/__main__.py` to support `python -m autosubmit`.

## Non-Goals

- **Tier 3 Structural Overhauls**: Re-architecting directory partitioning for logs, rewriting database layers, or replacing frame introspection in `parameters.py` are out of scope.
- **Configuration Schema Alterations**: No user-facing YAML keys or Autosubmit CLI parameters will be renamed or deprecated.
- **Scheduler Polling Redesign**: Changing Autosubmit's iterative polling model into an event-driven daemon is out of scope.
- **Replacing Python Packaging**: Mandating `uv` for end-user execution or compute-node execution is out of scope.

## Open Questions

None. All architectural alternatives and implementation options were evaluated and resolved during drafting.

## Critical User Journeys (CUJs)

### CUJ 1: Workflow Creation and Initialization
- **Given**: An experiment configuration defining 10,000 simulation chunks.
- **When**: An operator executes `autosubmit create <expid> --noplot` or starts `autosubmit run <expid>`.
- **Then**: The graph and jobs instantiate without repeated disk reads of `.autosubmitrc`, completing initialization within ~6–7 seconds instead of ~16 seconds.

### CUJ 2: Low-Latency Polling and Job Succession
- **Given**: `autosubmit run` is polling an experiment with 10,000 jobs.
- **When**: A batch of jobs finishes on the remote cluster.
- **Then**: Autosubmit resolves completed dependencies using $O(1)$ lookups in milliseconds and dispatches successor jobs to Slurm without a 10-second polling freeze.

### CUJ 3: Status and Statistics Inspection
- **Given**: An operator inspects metrics or status for an experiment with 10,000 jobs.
- **When**: The operator executes `autosubmit stats <expid> --hide` or `autosubmit monitor <expid> --hide -txt`.
- **Then**: `autosubmit stats` completes in under 2 seconds without freezing the CPU, and `autosubmit monitor` prints deep 1,000-chunk trees without raising a `RecursionError`.

### CUJ 4: Log Inspection with Remote Compression
- **Given**: An experiment runs with remote log compression enabled.
- **When**: An operator runs `autosubmit cat-log <expid>_<job_name>_run` to inspect a failed or running job.
- **Then**: `cat-log` locates the `.gz` log file, decompresses it during stream reading, and outputs the contents directly to terminal standard output.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

The Tier 1 and Tier 2 remediations modify five core subsystems within Autosubmit without changing external schemas or inter-process protocols:

```mermaid
flowchart TD
    subgraph Configuration["Configuration Subsystem"]
        RC[".autosubmitrc"] -->|Single Read| BC["BasicConfig (Cached in Memory)"]
        BC -.->|Static Paths| JobInit["Job.__init__() (No Disk I/O)"]
        YAML["Experiment YAMLs"] -->|Single Reload| Stats["autosubmit stats (Pre-loop Reload)"]
    end

    subgraph WorkflowEngine["Workflow & Graph Engine"]
        JobList["JobList Instance"] -->|Direct Attribute Access| GraphNodes["self.graph.nodes[name]['job']"]
        GraphNodes -->|O(1) Hash Lookup| DepRes["update_list() & Dependency Resolution"]
        GraphHierarchy["Job Graph Hierarchy"] -->|Iterative Worklist (Deque)| Monitor["_recursion_print() (No Call Stack Overflow)"]
    end

    subgraph LoggingStorage["Logging & Remote Storage"]
        RemotePlatform["Remote HPC Platform"] -->|compress_remote_logs = True| GzLogs["Compressed Remote Logs (*.gz)"]
        GzLogs -->|File Transfer| LocalLogs["Local Storage: tmp/LOG_{expid} (*.gz)"]
        LocalLogs -->|Suffix Detection & gzip.open| CatLog["autosubmit cat-log (Decompress on Demand)"]
        LogModule["autosubmit.log.Log"] -->|WARNING=4000 above INFO=3000| ConsoleLog["Corrected Severity Filtering"]
        Daemon["autosubmit run"] -->|Default: INFO (50 GB to under 100 MB)| RunLog["_run.log File"]
    end

    subgraph EntryPoint["Packaging & Entry Point"]
        PyCLI["python -m autosubmit"] --> MainPy["autosubmit/__main__.py"]
        MainPy --> ASMain["autosubmit.scripts.autosubmit:main"]
    end
```

### 1. Configuration Caching (`BasicConfig`)
- **Modules**: `autosubmit/config/basicconfig.py`, `autosubmit/job/job.py`, `test/conftest.py`
- **Data Flow & Mechanism**:
  - `BasicConfig` is a class with static attributes representing system paths (`LOCAL_ROOT_DIR`, `DB_PATH`, `GLOBAL_LOG_DIR`).
  - Store a copy of initial default attributes at class definition time (`_initial_defaults: dict[str, Any] = {}`).
  - Introduce an internal flag `_is_read: bool = False` and an optional parameter `BasicConfig.read(force: bool = False)`.
  - Exclude single-underscore attributes in `BasicConfig.props()` so internal cache attributes do not leak into configuration exports.
  - When `BasicConfig.read()` executes, the method checks `_is_read`. If `_is_read` is `True` and `force` is `False`, the method returns immediately.
  - Upon completing configuration parsing, the method sets `_is_read = True`.
  - Implement `BasicConfig.reset()` to clear `_is_read = False`, set `CONFIG_FILE_FOUND = False`, and restore cached attributes to values from `_initial_defaults`.
  - Update `test/conftest.py:initialize_autosubmitrc` to call `BasicConfig.reset()` before `read()`, preserving fixture isolation across pytest tests.
  - In `autosubmit/job/job.py:506`, retain `BasicConfig.read()` in `Job.__init__()`. All instantiated jobs will access cached attributes in memory without disk I/O.

### 2. Direct Graph Job Lookup (`JobList.get_job_by_name`)
- **Module**: `autosubmit/job/job_list.py`
- **Data Flow & Mechanism**:
  - `JobList` currently derives `self.job_list` via a list comprehension over `self.graph.nodes(data=True)` on every access. In `get_job_by_name(name)`, it executes a linear scan over that newly allocated list.
  - NetworkX `self.graph.nodes` is already a dictionary keyed by job name, storing the job instance under the key `'job'`.
  - Refactor `JobList.get_job_by_name(self, name: str) -> Job | None`:
    ```python
    node_data = self.graph.nodes.get(name)
    return node_data.get("job") if node_data else None
    ```
  - This provides $O(1)$ lookup performance in two lines of code without introducing separate index dictionaries or requiring synchronization across `add_job`, `remove_job`, `reset_jobs`, or loaders.

### 3. Decoupling Configuration Reloading from Parameter Assignment
- **Modules**: `autosubmit/job/job.py`, `autosubmit/workflow/manage.py`
- **Data Flow & Mechanism**:
  - In `autosubmit/job/job.py:2522`, `Job.update_parameters()` unconditionally executes `as_conf.reload()` when `set_attributes=True`.
  - An individual job instance must not reload the shared workflow configuration from disk.
  - Remove `as_conf.reload()` from `Job.update_parameters()`.
  - In `autosubmit/workflow/manage.py:1380` (`stats()`):
    - Call `as_conf.reload()` once before the job iteration loop.
    - Keep `job.update_parameters(as_conf, set_attributes=True)` inside the loop so that job attributes (`wallclock`, `processors`, `splits`, and stat files) are correctly initialized using the in-memory configuration without re-parsing YAML files from disk.

### 4. Iterative Tree Printing (`autosubmit monitor`)
- **Module**: `autosubmit/job/job_list.py`
- **Data Flow & Mechanism**:
  - `JobList._recursion_print` currently implements a recursive depth-first search that pushes a stack frame per sequential chunk, crashing Python's recursion limit at chunk 1,000.
  - Retain the existing method signature:
    `_recursion_print(self, job, level, visited=None, status_change=None, nocolor=False) -> str`.
  - Replace the recursive call stack with an explicit `collections.deque` worklist.
  - When collecting children of a job, sort them and push them onto the stack in reverse order (`reversed(sorted(children, ...))`) to preserve pre-order branch display sequence.
  - Track `visited` nodes in an internal set for $O(1)$ cycle detection. If caller passes `visited` as a list, append visited job names to maintain caller list mutation contracts.

### 5. Remote Log Compression & Transparent CLI Viewing
- **Modules**: `autosubmit/platforms/platform.py`, `autosubmit/log/catlog.py`
- **Data Flow & Mechanism**:
  - In `autosubmit/platforms/platform.py:240`, set `self.compress_remote_logs = True`.
  - In `autosubmit/platforms/platform.py:242`, set `self.compression_level = 6`.
  - In `autosubmit/platforms/platform.py:247`, set the fallback default to `True`:
    `self.compress_remote_logs = platform_config.get("COMPRESS_REMOTE_LOGS", True)`.
  - In `autosubmit/platforms/platform.py:249`, set the fallback default to `6`:
    `self.compression_level = platform_config.get("COMPRESSION_LEVEL", 6)`.
  - Remote execution scripts compress remote logs with `gzip -6`, reducing scratch and transferred bytes by 83.4% without the 3.6x CPU penalty of `gzip -9`.
  - In `autosubmit/log/catlog.py`:
    - Broaden file pattern matching from `f"{exp_or_job_id}.*.{extension}"` to `f"{exp_or_job_id}.*.{extension}*"`.
    - In `_view_file(log_file, mode)`:
      - If `log_file.suffix == ".gz"`:
        - For `ViewMode.CAT`: Open with `gzip.open(log_file, "rt", encoding="utf-8", errors="replace")` and stream output directly to `sys.stdout`. Return `0`.
        - For `ViewMode.TAIL`: Decompress and write the last 20 lines to `sys.stdout`. Display notice: `Log.info("Note: Live follow (-m t / tail -f) is not supported on compressed archives.")`. Return `0`.
      - If uncompressed, continue using existing `subprocess` execution (`cat` and `tail`). Return process exit code.

### 6. Logging Subsystem Severity Alignment
- **Modules**: `autosubmit/log/log.py`, `autosubmit/scripts/autosubmit.py`
- **Data Flow & Mechanism**:
  - Re-align integer constants in `autosubmit/log/log.py`:
    ```python
    DEBUG = 2000
    INFO = 3000
    WARNING = 4000
    RESULT = 5000
    ERROR = 6000
    CRITICAL = 7000
    ```
  - Update internal severity lookup table in `set_file()` so that setting log level `INFO` (3000) permits `WARNING` (4000), `ERROR` (6000), and `CRITICAL` (7000) to pass through to disk.
  - Update `Log.printlog()`: set default `code=3000`, route `3000 <= code < 4000` to `Log.info`, and route `4000 <= code < 5000` to `Log.warning`.
  - In `autosubmit/scripts/autosubmit.py:93`, set the default `--logfile` option from `DEBUG` to `INFO`.
  - In `autosubmit/scripts/autosubmit.py:101`, set the default `--logconsole` option from `WARNING` to `INFO`.
  - Allow users to pass `--logfile DEBUG` (`-lf DEBUG`) or `--logconsole DEBUG` (`-lc DEBUG`) on the command line for detailed diagnostic tracing.

### 7. Module Entry Point (`autosubmit/__main__.py`)
- **Module**: `autosubmit/__main__.py`
- **Data Flow & Mechanism**:
  - Create `autosubmit/__main__.py`:
    ```python
    from autosubmit.scripts.autosubmit import main

    if __name__ == "__main__":
        main()
    ```
  - This module allows invocation via `python -m autosubmit [command]` across containerized and isolated Python environments.

--------------------------------------------------------------------------------

## Alternatives Considered

| Alternative | Summary | Reason for Rejection |
| :--- | :--- | :--- |
| **A: Relying on `uv run`** | Run CLI commands via `uv run` wrapper | Adds 38–41 ms fixed overhead per command. HPC compute nodes lack internet access for environment sync checks. |
| **B: Shelling out to `zcat`** | Invoke system `zcat` subprocess | Platform portability divergence across Linux, macOS, and BSD. Spawning subprocesses adds latency and escaping risks. |
| **C: `@lru_cache` on `BasicConfig`** | Use standard library `@lru_cache` | Hidden C-level cache causes test fixture pollution. An explicit `_is_read` flag with `BasicConfig.reset()` provides deterministic cleanup. |
| **D: Zstandard (`zstd`) for Logs** | Use Zstandard instead of gzip | Requires compiling third-party C dependencies on remote HPC clusters. Gzip is pre-installed on Linux HPC environments and standard in Python. |
| **E: Auxiliary Index Dictionary** | Add `self._jobs_by_name: dict[str, Job]` | Duplicates state with NetworkX `self.graph.nodes`. Direct query on `self.graph.nodes.get(name)['job']` gives $O(1)$ without synchronization bugs. |
| **F: Removing `BasicConfig.read()`** | Omit read call from `Job.__init__` | Leaves standalone unit tests vulnerable to uninitialized paths. The idempotent `_is_read` guard ensures safety for both CLI and standalone tests. |

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Performance & Latency
- **Workflow Creation**: `BasicConfig.read()` disk overhead in `autosubmit create` must drop from 6.62s to < 0.005s for 10,000 jobs.
- **Dependency Lookup**: `JobList.get_job_by_name()` must execute in $O(1)$ time (< 0.001 ms per lookup).
- **Statistics Computation**: `autosubmit stats` execution time must drop from ~62 seconds to < 2.0 seconds on 10,000 jobs.
- **Log Footprint**: Remote log compression must achieve > 80% volume reduction (~834 GB saved per 1 TB).

### Reliability & Backward Compatibility
- **Log Viewer Compatibility**: `autosubmit cat-log` must open both legacy uncompressed logs and `.gz` compressed logs without requiring user intervention.
- **Backward Compatibility**: Existing `.autosubmitrc` files, experiment YAML configurations, and SQLite database schemas must remain compatible.
- **Test Isolation**: `BasicConfig.reset()` must guarantee no state leakage across pytest execution sessions.

### Security & Integrity
- In-process gzip decompression via Python's standard `gzip` module avoids spawning shell subprocesses, eliminating shell escaping vulnerabilities.

--------------------------------------------------------------------------------

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
| :--- | :--- | :--- | :--- |
| **Test State Pollution**: `BasicConfig` caching leaks modified paths between pytest fixtures. | High | Medium | Implement `BasicConfig.reset()` restoring initial class defaults; call in `initialize_autosubmitrc` fixture in `test/conftest.py`. |
| **Console Output Muting**: Re-aligning log levels without updating `--logconsole` default silences progress. | High | High | Update `--logconsole` default to `"INFO"` in `autosubmit/scripts/autosubmit.py:101`. |
| **Tail Mode on Compressed Logs**: `cat-log -m t` cannot follow active binary `.gz` streams. | Medium | Medium | In `catlog.py`, detect `.gz` in tail mode, output the last 20 lines, and display an informational note explaining live follow limitation. |
| **Platform Config Invalidation**: Missing `COMPRESS_REMOTE_LOGS` in `platforms.yml` reverts setting to `False`. | Medium | High | Set fallback default to `True` in `platform_config.get("COMPRESS_REMOTE_LOGS", True)` at `platform.py:247`. |
| **DFS Tree Ordering Shift**: Iterative stack reverses child branch display order in `monitor`. | Low | Medium | Push sorted child nodes in reverse order (`reversed(sorted(...))`) onto the worklist deque. |

--------------------------------------------------------------------------------

## Additional Resources

- Autosubmit Performance & Storage Investigation Report: [AUTOSUBMIT_PERFORMANCE_AND_LOG_INVESTIGATION_REPORT.md](../../AUTOSUBMIT_PERFORMANCE_AND_LOG_INVESTIGATION_REPORT.md)
- GitHub Issue #3384: `autosubmit setstatus` updated_list enhancement: [Issue #3384](https://github.com/BSC-ES/autosubmit/issues/3384)

--------------------------------------------------------------------------------

## Future Improvements

- **Tier 3 Log Partitioning**: Subdirectory partitioning for `tmp/LOG_<expid>` to avoid reaching filesystem directory entry limits on multi-month runs (>100,000 files).
- **SQLite Index Optimization**: Dedicated index structures on `job_list.db` to accelerate state transition queries during polling cycles with over 10,000 jobs.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Target Files to Create/Modify

#### 1. `autosubmit/config/basicconfig.py` (Modify)
- **Type / Scope**: Core configuration singleton
- **Proposed Changes**:
  - Store a copy of initial class attributes at class definition time:
    ```python
    _initial_defaults = {
        k: v for k, v in vars().items()
        if not k.startswith("_") and not inspect.isroutine(v)
    }
    _is_read: bool = False
    ```
  - Update `BasicConfig.props()` to ignore attributes starting with `_`.
  - Update signature to `read(force: bool = False)`.
  - At start of `read()`, check `if BasicConfig._is_read and not force: return`.
  - Set `BasicConfig._is_read = True` at conclusion of successful path parsing.
  - Add classmethod `reset()` that sets `_is_read = False`, sets `CONFIG_FILE_FOUND = False`, and restores path attributes to values from `_initial_defaults`.
- **Rationale**: Prevents repeated parsing of `.autosubmitrc` from disk across thousands of object initializations while preserving explicit test fixture control.

#### 2. `test/conftest.py` (Modify)
- **Type / Scope**: Pytest test configuration and fixtures
- **Proposed Changes**:
  - In `initialize_autosubmitrc(tmp_path)` fixture (around line 164), add `BasicConfig.reset()` immediately before `BasicConfig.read()`.
- **Rationale**: Guarantees test fixture isolation so every test function reads its own temporary configuration without state pollution.

#### 3. `autosubmit/job/job.py` (Modify)
- **Type / Scope**: Job data model
- **Proposed Changes**:
  - In `Job.__init__()` line 506, retain the `BasicConfig.read()` call.
  - In `update_parameters(self, as_conf: 'AutosubmitConfig', set_attributes: bool = False, reset_logs: bool = False) -> dict`:
    - Remove `as_conf.reload()` from line 2522.
- **Rationale**: Eliminates redundant disk parsing during job instantiation. Prevents per-job YAML reloads during bulk parameter updates.

#### 4. `autosubmit/log/log.py` (Modify)
- **Type / Scope**: Logging subsystem
- **Proposed Changes**:
  - Re-align integer log level constants:
    `DEBUG = 2000`, `INFO = 3000`, `WARNING = 4000`, `RESULT = 5000`, `ERROR = 6000`, `CRITICAL = 7000`.
  - In `set_file()`, update `levels` dictionary to match the corrected integer ordering.
  - In `Log.printlog()`: set default argument `code=3000`. Route `3000 <= code < 4000` to `Log.info`. Route `4000 <= code < 5000` to `Log.warning`.
- **Rationale**: Fixes severity scale inversion where `WARNING` (3000) was lower than `INFO` (4000), restoring warning log messages to console and disk outputs.

#### 5. `autosubmit/scripts/autosubmit.py` (Modify)
- **Type / Scope**: Main CLI parser
- **Proposed Changes**:
  - In `_get_arg_parser()`, update `--logfile` option default value from `"DEBUG"` to `"INFO"`.
  - In `_get_arg_parser()`, update `--logconsole` option default value from `"WARNING"` to `"INFO"`.
- **Rationale**: Reduces `_run.log` disk size from ~50 GB to < 100 MB while ensuring informational progress messages display on the terminal.

#### 6. `autosubmit/__main__.py` (Create)
- **Type / Scope**: Module execution entry point
- **Proposed Changes**:
  - Create top-level `autosubmit/__main__.py`:
    ```python
    from autosubmit.scripts.autosubmit import main

    if __name__ == "__main__":
        main()
    ```
- **Rationale**: Enables standard `python -m autosubmit` execution for containerized environments and virtual environment runners.

#### 7. `autosubmit/workflow/manage.py` (Modify)
- **Type / Scope**: Workflow statistics and execution
- **Proposed Changes**:
  - In `stats()` around line 1380:
    ```python
    as_conf.reload()
    for job in job_list.get_job_list():
        job.update_parameters(as_conf, set_attributes=True)
    ```
  - Insert `as_conf.reload()` once before entering the job loop, and retain `set_attributes=True` inside the loop.
- **Rationale**: Eliminates $O(N)$ redundant disk reloads in `autosubmit stats` (reducing benchmark runtime from 61.7 s to < 2.0 s on 10,013 jobs). Preserves job attribute assignments.

#### 8. `autosubmit/job/job_list.py` (Modify)
- **Type / Scope**: Graph data model and traversal
- **Proposed Changes**:
  - In `get_job_by_name(self, name: str) -> Job | None`:
    ```python
    node_data = self.graph.nodes.get(name)
    return node_data.get("job") if node_data else None
    ```
  - Replace recursive DFS in `_recursion_print(self, job, level, visited=None, status_change=None, nocolor=False)` with an iterative worklist using `collections.deque`.
  - Push sorted children in reverse order (`reversed(sorted(...))`) onto the deque.
  - Use an internal `set` for $O(1)$ loop checks. If `isinstance(visited, list)`, append visited job names in place.
- **Rationale**: Provides $O(1)$ job lookup via existing graph node attributes without duplicate indexing state. Prevents `RecursionError` exceptions on deep dependency chains.

#### 9. `autosubmit/platforms/platform.py` (Modify)
- **Type / Scope**: Platform remote execution engine
- **Proposed Changes**:
  - In `Platform.__init__()` line 240, change default `self.compress_remote_logs = False` to `self.compress_remote_logs = True`.
  - In `Platform.__init__()` line 242, change default `self.compression_level = 9` to `self.compression_level = 6`.
  - In `Platform.__init__()` line 247, change fallback default to `True`:
    `self.compress_remote_logs = platform_config.get("COMPRESS_REMOTE_LOGS", True)`.
  - In `Platform.__init__()` line 249, change fallback default to `6`:
    `self.compression_level = platform_config.get("COMPRESSION_LEVEL", 6)`.
- **Rationale**: Compresses remote job logs before file transfer with gzip level 6 when omitted from `platforms.yml`. Reduces log storage footprint (observed 83.4% reduction in benchmark tests).

#### 10. `autosubmit/log/catlog.py` (Modify)
- **Type / Scope**: Log viewer utility
- **Proposed Changes**:
  - In `_find_job_log`: Broaden pattern search from `f"{exp_or_job_id}.*.{extension}"` to `f"{exp_or_job_id}.*.{extension}*"`.
  - In `_view_file(log_file, mode)`:
    - If `log_file.suffix == ".gz"`:
      - If `mode == ViewMode.CAT`: Open via `gzip.open(log_file, "rt", encoding="utf-8", errors="replace")` and stream to `sys.stdout`. Return `0`.
      - If `mode == ViewMode.TAIL`: Read and print the last 20 lines of the decompressed stream. Display notice: `Log.info("Note: Live follow (-m t / tail -f) is not supported on compressed archives.")`. Return `0`.
    - If uncompressed, continue using existing `subprocess` execution (`cat` and `tail`). Return process exit code.
- **Rationale**: Ensures `autosubmit cat-log` displays plain text and gzipped logs in cat and tail modes.

#### 11. `test/unit/config/test_basicconfig.py` (Modify)
- **Type / Scope**: Unit test suite
- **Proposed Changes**:
  - Add test `test_basic_config_read_idempotent_caching` to verify that repeated calls do not re-read from disk.
  - Add test `test_basic_config_read_force_reload` to verify that `force=True` re-reads from disk.
  - Add test `test_basic_config_reset` to verify that `BasicConfig.reset()` clears `_is_read` and restores original class defaults.

#### 12. `test/unit/test_job_list.py` (Modify)
- **Type / Scope**: Unit test suite
- **Proposed Changes**:
  - Add test `test_get_job_by_name_o1_lookup` to verify that `get_job_by_name` retrieves jobs via `self.graph.nodes`.
  - Add test `test_recursion_print_deep_chain`: Compare iterative output against recursive output on a small graph of 50 jobs. Verify that a 1,500-job linear chain completes without `RecursionError` and outputs all job lines.

#### 13. `test/unit/log/test_log.py` (Modify)
- **Type / Scope**: Unit test suite
- **Proposed Changes**:
  - Add test to verify that `Log.WARNING > Log.INFO`.
  - Add test to verify that `Log.warning` messages are emitted when log filter level is set to `INFO`.
  - In `test_printlog()`, update expected parameters to reflect `code=3000` for info and `code=4000` for warning.

#### 14. `test/unit/log/test_catlog.py` (Create)
- **Type / Scope**: Unit test suite
- **Proposed Changes**:
  - Add tests to verify that `cat_log` displays `.out`, `.err`, `.out.gz`, and `.err.gz` files in `cat` and `tail` modes.

#### 15. `test/unit/test_commands.py` (Modify)
- **Type / Scope**: CLI entry point test suite
- **Proposed Changes**:
  - Add test to verify that `python -m autosubmit --version` executes and returns exit code 0.

---

### Verification Plan

#### Static Analysis & Compilation
- Execute `python3 -m py_compile` on all modified Python files to ensure syntax validity.
- Run `uv run --extra lint ruff check` on all modified files to verify linting and formatting compliance.

#### Unit Testing
- Run pytest targeting the modified test suites:
  ```bash
  uv run --extra tests pytest -n0 test/unit/config/test_basicconfig.py \
                                   test/unit/test_job_list.py \
                                   test/unit/log/test_log.py \
                                   test/unit/log/test_catlog.py \
                                   test/unit/test_commands.py
  ```
- Run the full project test suite (`uv run --extra tests pytest -n0 test/unit/`) to ensure no regressions across existing components.

#### Empirical Performance Verification
Use synthetic experiment `a002` (10,013 jobs) for performance verification:
1. Run `autosubmit create a002 --noplot -f`: verify wall time drops from ~7.0s to ~0.5s.
2. Run `autosubmit stats a002 --hide`: verify wall time drops from ~61.7s to < 2.0s.
3. Run `autosubmit monitor a002 --hide -txt`: verify command returns exit code 0 on the 1,000-chunk workflow.
4. Run `autosubmit cat-log <job_id> -f o -m c` and `autosubmit cat-log <job_id> -f o -m t`: verify display of `.gz` compressed job logs in cat and tail modes.
