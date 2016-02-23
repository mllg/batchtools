#' @title ClusterFunctions for Local Multicore Execution
#'
#' @description
#' Jobs are spawned by starting multiple R sessions on the command line.
#'
#' @param ncpus [\code{integer(1)}]\cr
#'   Number of VPUs of worker.
#'   Default is to use all cores but one, where total number of cores "available" is given by option \code{mc.cores}
#'   and defaults to the heuristic implemented in \code{\link[parallel]{detectCores}}.
#' @param max.load [\code{numeric(1)}]\cr
#'   Load average (of the last 5 min) at which the worker is considered occupied,
#'   so that no job can be submitted. Default is \code{Inf}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsMulticore = function(ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L), max.load = Inf) {
  if (.Platform$OS.type == "windows")
    stop("clusterFunctionsMulticore not compatible with Windows")
  worker = makeWorker("localhost", ncpus, max.load)

  submitJob = function(reg, jc) {
    updateWorker(worker, reg)
    if (worker$available == "avail") {
      pid = try(startWorkerJob(worker, reg, jc$uri, jc$log.file))
      if (is.error(pid)) {
        makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = "Submit failed.")
      } else {
        worker$available = "unknown"
        makeSubmitJobResult(status = 0L, batch.id = pid)
      }
    } else {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = sprintf("Busy: %s", worker$available))
    }
  }

  killJob = function(reg, batch.id) {
    killWorkerJob(worker, reg, batch.id)
  }

  listJobsRunning = function(reg) {
    listWorkerJobs(worker, reg)
  }

  makeClusterFunctions(name = "Multicore", submitJob = submitJob, killJob = killJob,
    listJobsRunning = listJobsRunning, store.job = TRUE)
}
