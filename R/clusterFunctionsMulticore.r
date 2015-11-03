#' @title Cluster Functions for Local Multicore Execution
#'
#' @description
#' Jobs are spawned by starting multiple R sessions on the command line
#' (similar like on true batch systems).
#'
#' @param ncpus [\code{integer(1)}]\cr
#'   Number of VPUs of worker.
#'   Default is to use all cores but one, where total number of cores
#'   "available" is given by option \code{\link[base:options]{mc.cores}}
#'   and if that is not set it is inferred by
#'   \code{\link[parallel]{detectCores}}.
#' @param max.jobs [\code{integer(1)}]\cr
#'   Maximal number of jobs that can run concurrently for the current registry.
#'   Default is \code{ncpus}.
#' @param max.load [\code{numeric(1)}]\cr
#'   Load average (of the last 5 min) at which the worker is considered occupied,
#'   so that no job can be submitted.
#'   Default is inferred by \code{\link[parallel]{detectCores}}, cf. argument \code{ncpus}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsMulticore = function(ncpus = max(getOption("mc.cores", parallel::detectCores()) - 1L, 1L), max.jobs, max.load) {
  if (.Platform$OS.type == "windows")
    stop("clusterFunctionsMulticore do not work in Windows")
  worker = makeWorker("localhost", ncpus, max.jobs, max.load)

  submitJob = function(reg, jc) {
    updateWorker(worker, reg$file.dir, tdiff = 0L)
    s = worker$available
    if (s != "A") {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = sprintf("Busy: %s", s))
    } else {
      pid = try(startWorkerJob(worker, jc$uri, jc$log.file))
      if (is.error(pid))
        makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = "Submit failed.")
      else
        makeSubmitJobResult(status = 0L, batch.id = pid)
    }
  }

  killJob = function(reg, batch.id) {
    killWorkerJob(worker, batch.id)
  }

  listJobs = function(reg) {
    listWorkerJobs(worker, reg$file.dir)
  }

  makeClusterFunctions(name = "Multicore", submitJob = submitJob, killJob = killJob,
    listJobs = listJobs, store.job = TRUE)
}
