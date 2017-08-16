# batchtools 0.9.5

* Added a missing routine to upgrade registries created with batchtools prior to v0.9.3.
* Fixed a bug where the registry could not be synced if jobs failed during initialization (#135).
* The sleep duration used in `waitForJobs()` and `submitJobs()` can now be set via the configuration file.
* A new heuristic will try to detect if the registry has been altered by a simultaneously running R session.
  If this is detected, the registry will be set to a read-only state.
* New argument `writeable` for `loadRegistry()` to allow loading registries explicitly as read-only.
* Removed argument `update.paths` from `loadRegistry()`.

# batchtools 0.9.4

* Fixed handling of `file.dir` with special chars like whitespace.
* All backward slashes will now be converted to forward slashes on windows.
* Fixed order of arguments in `findExperiments()` (argument `ids` is now first).
* Removed code to upgrade registries created with versions prior to v0.9.0 (first CRAN release).
* `addExperiments()` now warns if a design is passed as `data.frame` with factor columns and `stringsAsFactors` is `TRUE`.
* Added functions `setJobNames()` and `getJobNames()` to control the name of jobs on batch systems.
  Templates should be adapted to use `job.name` instead of `job.hash` for naming.
* Argument `flatten` of `getJobResources()`, `getJobPars()` and `getJobTable()` is deprecated and will be removed.
  Future versions of the functions will behave like `flatten` is set to `FALSE` explicitly.
  Single resources/parameters must be extracted manually (or with `tidyr::unnest()`).

# batchtools 0.9.3

* Running jobs now are also included while querying for status "started". This affects `findStarted()`, `findNotStarted()` and `getStatus()`.
* `findExperiments()` now performs an exact string match (instead of matching substrings) for patterns specified via `prob.name` and `algo.name`.
  For substring matching, use `prob.pattern` or `algo.pattern`, respectively.
* Changed arguments for `reduceResultsDataTable()`
    * Removed `fill`, now is always `TRUE`
    * Introduced `flatten` to control if the result should be represented as a column of lists or flattened as separate columns.
      Defaults to a backward-compatible heuristic, similar to `getJobPars`.
* Improved heuristic to lookup template files.
  Templates shipped with the package can now be used by providing just the file name (w/o extension).
* Updated CITATION

# batchtools 0.9.2

* Full support for array jobs on Slurm and TORQUE.
* Array jobs have been disabled for SGE and LSF (due to missing information about the output format) but will be re-enable in a future release.
  Note that the variable `n.array.jobs` has been removed from `JobCollection` in favor of the new variable `array.jobs` (logical).
* `findExperiments()` now has two additional arguments to match using regular expressions.
  The possibility to prefix a string with "~" to enable regular expression matching has been removed.
* New function `batchReduce()`.
* New function `estimateRuntimes()`.
* New function `removeRegistry()`.
* Missing result files are now handled more consistently, raising an exception in its defaults if the result is not available.
  The argument `missing.val` has been added to `reduceResultsList()` and `reduceResultsDataTable()` and removed from `loadResult()` and `batchMapResults()`.
* Timestamps are now stored with sub-second accuracy.
* Renamed Torque to TORQUE. This especially affects the constructor `makeClusterFunctionsTorque` which now must be called via `makeClusterFunctionsTORQUE()`
* `chunkIds()` has been deprecated. Use `chunk()`, `lpt()` or `binpack()` instead.
* Fixed listing of jobs for `ClusterFunctionsLSF` and `ClusterFunctionsOpenLava` (thanks to @phaverty).
* Job hashes are now prefixed with the literal string 'job' to ensure they start with a letter as required by some SGE systems.
* Fixed handling of `NULL` results in `reduceResultsList()`
* Fixed key lookup heuristic join functions.
* Fixed a bug where `getJobTable()` returned `difftimes` with the wrong unit (e.g., in minutes instead of seconds).
* Deactivated swap allocation for `ClusterFunctionsDocker`.
* The package is now more patient while communicating with the scheduler or file system by using a timeout-based approach.
  This should make the package more reliable and robust under heavy load.

# batchtools 0.9.0

Initial CRAN release.
See the vignette for a brief comparison with [BatchJobs](https://cran.r-project.org/package=BatchJobs)/[BatchExperiments](https://cran.r-project.org/package=BatchExperiments).
