### 4.1.15: Unreleased

The filter `-fp` of the command `autosubmit stats` changed in this release.
Previously, a `-fp 0` would not raise any errors, and would bring all the
jobs, just like when no filter is provided. Now both negative numbers and
`0` (zero) raise an error, and only values greater than `0` are used to compute
the filter the jobs. Not using any value for `-fp` still returns all jobs.

**Bug fixes:**

- Corrected the logic for handling `ZombieProcess` errors in `psutil` calls done in `autosubmit stop`, which
  prevented the command from working correctly if the experiment process appeared after a zombie in the list #2394
- Fixed an issue with Autosubmit's CITATION.cff that prevented Zenodo from automatically
  adding new deposits via its webhook #2401
- Deleted command `autosubmit test` that was not working in Autosubmit 4 #2386
- Removed PBS and SGE platforms as they are not working in AS4 #2349
- Log levels in the command line now accept `ERROR` #2412

**Enhancements:**

- EDITO Autosubmit-Demo container updated to install API in different environment #2398
- Update portalocker requirement from <=3.1.1 to <=3.2.0 #2423
- Fixed a `ZeroDivisionError` when using RO-Crate or `stats`, and also an issue
  where the message said `None` could not be iterable. #2389

### 4.1.14: Bug fixes, enhancements, and new features

**Bug fixes:**
- Fixed an issue with X11 calls causing errors. [#2324](https://github.com/BSC-ES/autosubmit/issues/2324)
- Resolved a problem where the Autosubmit monitor failed for non-owners of experiments. [#2272](https://github.com/BSC-ES/autosubmit/issues/2272)
- Corrected an issue where `experiment_data` was not saved when using a shared account. [PR](https://github.com/BSC-ES/autosubmit-config-parser/pull/82)
- Fixed timestamp-related issues in logs and processes. [#2275](https://github.com/BSC-ES/autosubmit/issues/2275), [PR](https://github.com/BSC-ES/autosubmit/pull/2284), [PR](https://github.com/BSC-ES/autosubmit/pull/2329)
- Reintroduced the `%SCRATCH_DIR%` variable and fixed problems with date variables. [#2248](https://github.com/BSC-ES/autosubmit/issues/2248), [PR](https://github.com/BSC-ES/autosubmit/pull/2292)
- Resolved an authentication issue related to user mapping. [PR #2333](https://github.com/BSC-ES/autosubmit/pull/2333)
- Fixed issues with wrapper not updating status correctly. [#2274](https://github.com/BSC-ES/autosubmit/issues/2274) [PR](https://github.com/BSC-ES/autosubmit/pull/2327)

**Enhancements:**
- Improved validation for the `expid` flag. [PR](https://github.com/BSC-ES/autosubmit/pull/2309)
- Made the details database more consistent and reliable. [PR](https://github.com/BSC-ES/autosubmit/pull/2296)
- Enhanced the flexibility of workflows, allowing for more adaptable configurations. [#2276](https://github.com/BSC-ES/autosubmit/issues/2276)
- Improved the log recovery logs. [PR](https://github.com/BSC-ES/autosubmit/pull/2341)

**New features:**
- Added support for `%^%` variables to improve template customization. [PR](https://github.com/BSC-ES/autosubmit/pull/2288), [Docs](https://autosubmit.readthedocs.io/en/latest/userguide/templates.html#sustitute-placeholders-after-all-files-have-been-loaded)
- Operational runs now require no pending commits, ensuring a cleaner workflow. [#2220](https://github.com/BSC-ES/autosubmit/issues/2220), [PR](https://github.com/BSC-ES/autosubmit/pull/2293)

### 4.1.13: Dependencies bug fixes, regression tests

- Fixed issues with dependencies not being correctly set or not being pruned. [#2184](https://github.com/BSC-ES/autosubmit/issues/2184) [PR](https://github.com/BSC-ES/autosubmit/pull/2241)
- Added dependencies regression test [PR](https://github.com/BSC-ES/autosubmit/pull/2241)

### 4.1.12: Logs, Memory, DB fixes. Enhancements, new features, and overall bug fixes.

**Mem fixes:** [PR](https://github.com/BSC-ES/autosubmit/pull/2130)
- Memory problems when running Autosubmit experiments on ClimateDT VM [#2122](https://github.com/BSC-ES/autosubmit/issues/2122)
  - Reduced job\_list, imports, log processors memory usage.
  - Memory leaks. [#2160](https://github.com/BSC-ES/autosubmit/issues/2160)

**Logs and db fixes:**
- Revive log process while Autosubmit is running [#2097](https://github.com/BSC-ES/autosubmit/pull/2097)
- CPU consumption fix [#1472](https://github.com/BSC-ES/autosubmit/issues/1472)
- Platforms don't recover logs and process exit prematurely [#1470](https://github.com/BSC-ES/autosubmit/issues/1470)
- Autosubmit remote log processes orphans [#1390](https://github.com/BSC-ES/autosubmit/issues/1390), [#1381](https://github.com/BSC-ES/autosubmit/issues/1381)
- Improve the log from log recovery processors [#2035](https://github.com/BSC-ES/autosubmit/issues/2035)

**Enhancements and new features:**
- Better log output on crash showing the command args and expid. [#2083](https://github.com/BSC-ES/autosubmit/issues/2083)
- Mail notifier now has the path to the logs and, optionally, the log attached [#1434](https://github.com/BSC-ES/autosubmit/issues/1434)
- Changed pip installation method from setup.py to pyproject.toml [#2180](https://github.com/BSC-ES/autosubmit/issues/2180), [#1384](https://github.com/BSC-ES/autosubmit/issues/1384)
- Autosubmit has a new entry point: autosubmit/scripts/autosubmit.py [#2182](https://github.com/BSC-ES/autosubmit/issues/2182)
- Fix typos in messages shown to users [#1469](https://github.com/BSC-ES/autosubmit/issues/1469)
- Each workflow change is tracked as long as it is committed and pushed. [#2213](https://github.com/BSC-ES/autosubmit/pull/2213), [#2179](https://github.com/BSC-ES/autosubmit/issues/2179)
- User mapping aka running under one robot account [#2009](https://github.com/BSC-ES/autosubmit/pull/2009)
- Allow pipeline commands by correcting the Autosubmit commands return values [#2124](https://github.com/BSC-ES/autosubmit/issues/2124)
- Allow users to create RO-Crate archives without archiving experiments [#1412](https://github.com/BSC-ES/autosubmit/issues/1412)

**Bug fixes:**
- autosubmit stop reworked to work with long names [#2104](https://github.com/BSC-ES/autosubmit/issues/2104)
- e-mail notification not working [#1483](https://github.com/BSC-ES/autosubmit/issues/1483)
- Weak dependencies in splits [#2067](https://github.com/BSC-ES/autosubmit/issues/2067)
- Current variables [#2004](https://github.com/BSC-ES/autosubmit/issues/2004)
- Recovery command issues [#1480](https://github.com/BSC-ES/autosubmit/issues/1480)
- Pip installation fixes [#1460](https://github.com/BSC-ES/autosubmit/issues/1460)
- Variable parsing fixes [#1466](https://github.com/BSC-ES/autosubmit/issues/1466), [#2065](https://github.com/BSC-ES/autosubmit/issues/2065)
- Splits="auto" fixes. [#1464](https://github.com/BSC-ES/autosubmit/issues/1464)
- Fugaku Header fixes [#1459](https://github.com/BSC-ES/autosubmit/issues/1459)
- Custom directives [#1457](https://github.com/BSC-ES/autosubmit/issues/1457)
- Job submission error handle changed from Critical -> Error [#2102](https://github.com/BSC-ES/autosubmit/issues/2102)
- Wrapper deadlock fixes.
- Fixed an issue with Autosubmit expid -y \$expid not copying 3.1X experiments. [#2073](https://github.com/BSC-ES/autosubmit/issues/2073)

**Autosubmit tests:**
- Multiple pull requests for unit tests, lint, vulture, and coverage improvements.
- Added integration tests for DB, logs, and autosubmit run.
- Improved the performance of the tests.

**Autosubmit config parser:**
- Updated ruamel.yaml to 0.18.8.
- Track workflow commits [#74](https://github.com/BSC-ES/autosubmit-config-parser/pull/74)
- Optimized the memory usage of the autosubmit config parser. [#70](https://github.com/BSC-ES/autosubmit-config-parser/pull/70)
- Export hpcarch parameters for the templates. [function](https://github.com/BSC-ES/autosubmit-config-parser/blob/fbd1f388ce57bd5d17f76bccea2aa02b0e2ab09a/autosubmitconfigparser/config/configcommon.py#L1876)
- Wallclock normalization and max\_wallclock > job wallclock error. [#67](https://github.com/BSC-ES/autosubmit-config-parser/pull/67)
- Added support for environment variables. These must be named as "AS\_ENV\_<VAR>" [#54](https://github.com/BSC-ES/autosubmit-config-parser/pull/54)
- Notify\_on, dependencies, "current\_" variables fixes. [#58](https://github.com/BSC-ES/autosubmit-config-parser/pull/58), [#53](https://github.com/BSC-ES/autosubmit-config-parser/pull/53), [#59](https://github.com/BSC-ES/autosubmit-config-parser/pull/59)

**Others:**
- All autosubmit projects moved to Github.
- Added Github actions for CI/CD.

4.1.11 - Enhancements, New Features, Documentation, and Bug Fixes
=================================================================

Enhancements and new features:

- #1444: Additional files now support YAML format.
- #1397: Use `tini` as entrypoint in our Docker image.
- #1207: Add experiment path in `autosubmit describe`.
- #1130: Now `autosubmit refresh` can clone without submodules.
- #1320: Pytest added to the CI/CD.
- #945: Fix portalocker releasing the lock when a portalocker exception
  is raised. Now it prints the warnings but only releases the lock when
  the command finishes (successfully or not).
- #1428: Use natural sort order for graph entries (adapted from Cylc),
  enable doctests to be executed with pytest.
- #1408: Updated our Docker image to work with Kubernetes.
  Included sample helm charts (tested with minikube).
- #1338: Stats can now be visualized in a PDF format.
- #1337: Autosubmit inspect, and other commands, are now faster.

Documentation and log improvements:
- #1274: A traceability section has been added.
- #1273: Improved AS logs by removing deprecated messages.
- #1394: Added Easybuild recipes for Autosubmit.
- #1439: Autosubmitconfigparser and ruamel.yaml updated.
- #1400: Fixes an issue with monitor and check experiment RTD section.
- #1382: Improved AS logs, removing paramiko warnings. (Updated paramiko version)
- #1373: Update installation instructions with `rsync`, and fix
  our `Dockerfile` by adding `rsync` and `subversion`.
- #1242: Improved warnings when using extended header/tailer.
- #1431: Updated the YAML files to use marenostrum5 instead of marenostrum4.

Bug fixes:
- #1423, #1421, and #1419: Fixes different issues with the split feature.
- #1407: Solves an issue when using one node.
- #1406: Autosubmit monitor is now able to monitor non-owned experiments again.
- #1317: Autosubmit delete now deletes the metadata and database entries.
- #1105: Autosubmit configure now admits paths that end with "/".
- #1045: Autosubmit now admits placeholders set in lists.
- #1417, #1398, #1287, and #1386: Fixes and improves the wrapper deadlock detection.
- #1436: Dependencies with Status=Running not working properly.
- (also enhancement) #1426: Fixes an issue with as_checkpoints.
- #1393: Better support for boolean YAML configurations.
- #1129: (Custom config) Platforms can now be defined under $expid/conf.
- #1443: Fixes an issue with additional files under bscearth000.

Others:

- #1427: Readthedocs works again.
- #1376: Autosubmit 4.1.9 was published to DockerHub.
- #1327: LSF platform has been removed.
- #1322: Autosubmit now has a DockerHub organization.
- #1123: Profiler can now be stopped.


4.1.10 - Hotfix
===============
- Fixed an issue with the performance of the log retrieval.
 
4.1.9 - Zombies, splits, tests and bug fixes
=====================================
- Splits calendar: Added a complete example to the documentation.
- Splits calendar: Fixed some issues with the split=" auto."
- Added two regression tests to check the .cmd with the default parameters.
- Fixed the command inspect without the -f.
- Yet another fix for zombie processors.
- Fixed the describe command when a user disappears from the system. 
- Fixes an issue with dependency not being linked.
- Docs improved.


4.1.8 - Bug fixes.
==================
- Fixed an issue with a socket connection left open.
- Fixed an issue with log recovery being disabled by default.
- Added exclusive parameter
- Fixed some X11 routines called by default


4.1.7 - X11, Migrate, script and Bug fixes
==========================================
- Migrate added, a feature to change the ownership of the experiments (/scratch and autosubmit_data paths)
- X11 added ( If you have to use an older version, after using this one. Autosubmit will complain until you perform create -f and recovery)
- Multiple QoL and bug fixes for wrappers and fixed horizontal ones.
- Added a new parameter `SCRIPT` that allows to write templates in the yaml config.
- Fixed an issue with STAT file.
- Fixed all issues related to zombie processors. 

4.1.6 - Bug fixes
=====================
- Fixed issues with additional files and dates
- Fixed issues with calendar splits
- Fixed issues with log processors
- Fixed issues with expid regex on --copy
- Added Autosubmit stop command

4.1.5 - PJS - Fugaku - Support
=====================
- Added Fugaku support.
- Enhanced the support for PJS.
- Added wrapper support for PJS scheduler
- Fixed issues with log traceback.

4.1.4 - Docs and Log Rework
=====================
- Log retrieval has been fully reworked, improving it is performance, FD, and memory usage.
- Fixed several issues with logs not being retrieved sometimes.
- Fixed several issues with job_data not being written correctly.
- Fixed some issues with retrials squashing stats/ logs.
- Added Marenostrum5 support.
- Fixed some issues with jobs inside a wrapper not having their parameters updated in realtime.
- Features a complete design rework of the autosubmit readthedocs 
- Fixed an issue with create without -f causing some jobs not having parent dependencies

4.1.3 - Bug fixes
=================
- Added Leonardo support.
- Improved inspect command.
- Added a new option to inspect.
- Wrapper now admits placeholders. 
- Reworked the wrapper deadlock code and total _jobs code. And fixed issues with "on_submission" jobs and wrappers. 
- Fixed issues with create without -f and splits.
- Improved error clarity.
- Added RO-Crate.
- Added Calendar for splits.




4.1.2 - Bug fixes
=================
- Fixed issues with version.
- Fixed issues with the duplication of jobs when using the heterogeneous option.
- Fixed some error messages.
- Fixed issues with monitoring non-owned experiments.

4.1.1 - Workflow optimizations and bug fixes
==========================================

Autosubmit supports much larger workflows in this version and has improved performance and memory usage. We have also fixed several bugs and added new features.

- Improved the performance and memory usage of the workflow generation process.
    -  Improved the performance and memory usage of the jobs generation process.
    -  Improved the performance and memory usage of the dependency generation process.
- Improved the performance and memory usage of the workflow visualization process.
- Added a new filter to setstatus ( -ftcs ) to filter by split.
- Added -no-requeue to avoid requeueing jobs.
- A mechanism was added to detect duplicate jobs.
- Fixed multiple issues with the splits usage.
- Fixed multiple issues with Totaljobs.
- Reworked the deadlock detection mechanism.
- Changed multiple debug messages to make them more straightforward.
- Changed the load/save pkl procedure
- Fixed issues with check command and additional files regex.
- Added the previous keyword.
- Fixed an issue with the historical db.
- Fixed an issue with historical db logs.
