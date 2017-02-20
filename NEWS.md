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
* Deactivated swap allocation for `clusterFunctionsDocker()`.
* The package is now more patient while communicating with the scheduler or file system by using a timeout-based approach.
  This should make the package more reliable and robust under heavy load.

# batchtools 0.9.0

Initial CRAN release.
See this [vignette](https://mllg.github.io/batchtools/articles/v01_Migration) for a brief comparison with [BatchJobs](https://cran.r-project.org/package=BatchJobs)/[BatchExperiments](https://cran.r-project.org/package=BatchExperiments).
