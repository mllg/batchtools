# batchtools 0.9.1

* `chunkIds()` has been deprecated. Use `chunk()`, `lpt()` or `binpack()` instead.
* `findExperiments()` now has two additional arguments to match for regular expressions.
  The possibility to prefix a string with "~" to enable regular expression matching has been removed.
* New function `estimateRuntimes()` which uses a random forest to predict runtimes of unfinished jobs.
* Fixed listing of jobs for `ClusterFunctionsLSF` and `ClusterFunctionsOpenLava` (thanks to @phaverty).
* Fixed broken key lookup in some join functions.
* Fixed a bug where `getJobTable()` returned `difftimes` with the wrong unit (e.g., in minutes instead of seconds).
* Deactivated swap allocation for clusterFunctionsDocker.
* Timestamps are now stored with sub-second accuracy.
* Job hashes are now prefixed with literal string 'job' to ensure they start with a letter as required by some SGE systems.

# batchtools 0.9.0

The packages [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) have been merged into batchtools.

## Internal changes in comparison with BatchJobs

* `batchtools` does not use SQLite anymore.
  Instead, all the information is stored directly in the registry using [data.tables](https://cran.r-project.org/package=data.table) acting as an in-memory database. As a side effect, many operations are much faster.
* Nodes do not have to access the registry.
  [submitJobs()](https://mllg.github.io/batchtools/reference/submitJobs) stores a temporary object of type [JobCollection](https://mllg.github.io/batchtools/reference/JobCollection) on the file system which holds all the information necessary to execute a chunk of jobs via [doJobCollection()](https://mllg.github.io/batchtools/reference/doJobCollection) on the node.
  This avoids file system locks because each job accesses only one file exclusively.
* `ClusterFunctionsMulticore` now uses the parallel package for multicore execution.
  `ClusterFunctionsSSH` can still be used to emulate a scheduler-like system which respects the work load on the local machine.

## User visible changes
* `batchtools` remembers the last created or loaded Registry and sets it as default registry.
  This way, you do not need to pass the registry around anymore.
  If you need to work with multiple registries simultaneously on the other hand, you can still do so by explicitly passing registries to the functions.
* The template file format has changed:
    - The scheduler should directly execute the command `Rscript -e 'batchtools::doJobCollection(<filename>)'`.
      There is no intermediate R source file like in BatchJobs.
    - All information stored in the object `JobCollection` can be accessed while brewing the template.
      Some variable names have changed and need to be adapted though.
      See the vignette on cluster functions for more information.
* Most functions now return a [data.table](https://cran.r-project.org/package=data.table) which is keyed with the `job.id`.
  This way, return values can be joined together easily and efficient (see this [help page](https://mllg.github.io/batchtools/reference/JoinTables) for some examples).
* The building blocks of a problem has been renamed from `static` and `dynamic` to the more intuitive `data` and `fun`.
  Thus, algorithm function should have the formal arguments `job`, `data` and `instance`.
* The function `makeDesign` has been removed.
  Parameters can be defined by just passing a `data.frame` or `data.table` to [addExperiments](https://mllg.github.io/batchtools/reference/addExperiments).
  For exhaustive designs, use `expand.grid()` or `data.table::CJ()`.

## New features
* Support for DockerSwarm via `ClusterFunctionsDocker`.
* Jobs can now be tagged and untagged to provide an easy way to group them.
* Some resources like the number of CPUs are now optionally passed to [parallelMap](https://cran.r-project.org/package=parallelMap).
  This eases nested parallelization, e.g. to use multicore parallelization on the slave by just setting a resource on the master.
  See [submitJobs()](https://mllg.github.io/batchtools/reference/submitJobs) for an example.
* `ClusterFunctions` are now more flexible in general as they can define hook functions which will be called at certain events.
  [ClusterFunctionsDocker](https://github.com/mllg/batchtools/blob/master/R/clusterFunctionsDocker.R) is an example use case which implements a housekeeping routine.
  This routine is called every time before a job is about to get submitted to the scheduler (in the case: the Docker Swarm) via the hook `pre.submit` and every time directly after the registry synchronized jobs stored on the file system via the hook `post.sync`.
