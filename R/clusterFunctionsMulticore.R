Multicore = R6Class("Multicore",
  cloneable = FALSE,
  public = list(
    pids = NULL,
    hashes = NULL,

    initialize = function(ncpus) {
      if (packageVersion("data.table") > "1.9.6")
        setthreads(1L)
      # FIXME: reset threads
      loadNamespace("parallel")
      self$pids = rep.int(NA_integer_, ncpus)
      self$hashes = character(ncpus)
      reg.finalizer(self, function(e) parallel::mccollect(self$pids[!is.na(self$pids)], wait = TRUE), onexit = FALSE)
    },

    spawn = function(jc) {
      if (!anyMissing(self$pids))
        while(!self$collect()) {}

      i = wf(is.na(self$pids))
      self$pids[i] = parallel::mcparallel(doJobCollection(jc, output = jc$log.file), mc.set.seed = FALSE)$pid
      self$hashes[i] = jc$job.hash
      invisible(jc$job.hash)
    },

    list = function() {
      self$collect()
      self$hashes[nzchar(self$hashes)]
    },

    collect = function() {
      results = unlist(filterNull(parallel::mccollect(self$pids[!is.na(self$pids)], wait = FALSE, timeout = 1)))
      i = which(self$hashes %in% results)
      self$pids[i] = NA_integer_
      self$hashes[i] = ""
      return(length(results) > 0L)
    }
  )
)

#' @title ClusterFunctions for Parallel Multicore Execution
#'
#' @description
#' Jobs are spawned asynchronously using the packages \pkg{parallel}.
#' Does not work on Windows, use \code{\link{makeClusterFunctionsSocket}} instead.
#'
#' @note
#' Sets the number of threads internally used by \pkg{data.table} to 1 during initialization (via \code{data.table::setthreads}).
#'
#' @param ncpus [\code{integer(1)}]\cr
#'   Number of VPUs of worker.
#'   Default is to use all cores. The total number of cores "available" is given by option \code{mc.cores}
#'   and defaults to the heuristic implemented in \code{\link[parallel]{detectCores}}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsMulticore = function(ncpus = NA_integer_) {
  if (testOS("windows"))
    stop("ClusterFunctionsMulticore do not support Windows. Use makeClusterFunctionsSocket instead.")
  assertCount(ncpus, positive = TRUE, na.ok = TRUE)
  if (is.na(ncpus)) {
    ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L)
    info("Auto-detected %i CPUs", ncpus)
  }
  p = Multicore$new(ncpus)

  submitJob = function(reg, jc) {
    p$spawn(jc)
    makeSubmitJobResult(status = 0L, batch.id = jc$job.hash, msg = "")
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list()
  }

  makeClusterFunctions(name = "Multicore", submitJob = submitJob, listJobsRunning = listJobsRunning,
    hooks = list(pre.sync = function(reg, fns) p$collect()), store.job = FALSE)
}
