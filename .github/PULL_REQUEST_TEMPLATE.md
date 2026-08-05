<!--

Thanks for your contribution! Please:
* List any related issues with a "closes" or "addresses" tag.
* Add a helpful title & description.
* Complete the checklist.

-->

**Check List**

- [ ] I have read `CONTRIBUTING.md`.
- [ ] Contains logically grouped changes (else tidy your branch by rebase).
- [ ] Does not contain off-topic changes (use other PRs for other changes).
- [ ] Applied any dependency changes to `pyproject.toml`.
- [ ] Tests are included (or explain why tests are not needed).
- [ ] Changelog entry included in `CHANGELOG.md` if this is a change that can affect users.
- [ ] Documentation updated.
- [ ] If this is a bug fix, PR should include a link to the issue (e.g. `Closes #1234`).

<!--
### Performance benchmarks

Autosubmit runs a performance gate on PRs labeled `perf-benchmark`: the PR is
benchmarked against the stored master baseline and the result is posted as a
comment.

* Add the `perf-benchmark` label if this PR can affect runtime performance
  (e.g. job list, monitor loop, database layer, wrappers, platform handling).
  Adding labels requires write/triage access -- if you can't, say so in the
  description so a maintainer can label it.
* Once labeled, the merge button stays disabled until the benchmark completes
  (full suite, ~20 minutes). Add the label when the PR is ready; while it
  is on, every push triggers another full run.
* Maintainers can comment `/metrics_full` for the heavy suite, and
  `/metrics_promote` to promote the last completed benchmark run as the new
  baseline (team members, and users listed as `@username` in the repo's
  `MAINTAINERS.md` on the default branch, only). `/metrics_promote` requires
  the last benchmark to have run on the current PR head — re-run `/metrics`
  (or `/metrics_full`) after any push.
* If the report shows regressions, review the comparison before merging:
  either fix the regression or explicitly accept the tradeoff.

See CONTRIBUTING.md > Benchmarks for details.
-->
