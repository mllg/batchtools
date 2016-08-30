Multicore = R6Class("Multicore",
  cloneable = FALSE,
  public = list(
    procs = NULL,
    ncpus = NA_integer_,

    initialize = function(ncpus) {
      if (packageVersion("data.table") > "1.9.6")
        setthreads(1L) # FIXME: reset threads
      loadNamespace("parallel")
      self$ncpus = ncpus
      self$procs = data.table(pid = integer(0L), hash = character(0L))
      # reg.finalizer(self, function(e) parallel::mccollect(self$procs$pid, wait = TRUE), onexit = FALSE)
    },

    spawn = function(jc) {
      force(jc)
      repeat {
        self$collect(0)
        if (nrow(self$procs) < self$ncpus)
          break
        Sys.sleep(1)
      }
      pid = parallel::mcparallel(doJobCollection(jc, output = jc$log.file), mc.set.seed = FALSE)$pid
      self$procs = rbind(self$procs, data.table(pid = pid, hash = jc$job.hash))
      invisible(as.character(pid))
    },

    list = function() {
      self$collect(1)
      as.character(self$procs$pid)
    },

    collect = function(timeout) {
      hashes = parallel::mccollect(wait = FALSE, timeout = timeout)
      if (!is.null(hashes)) {
        hashes = as.character(filterNull(hashes))
        self$procs = self$procs[hash %nin% hashes]
      }
      self$procs$pid
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
    force(jc)
    pid = p$spawn(jc)
    makeSubmitJobResult(status = 0L, batch.id = pid, msg = "")
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list()
  }

  makeClusterFunctions(name = "Multicore", submitJob = submitJob, listJobsRunning = listJobsRunning,
    hooks = list(pre.sync = function(reg, fns) p$collect(1)), store.job = FALSE)
}
