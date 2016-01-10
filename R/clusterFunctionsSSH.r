#' @title ClusterFunctions for Remote SSH Execution
#'
#' @description
#' Jobs are spawned by starting multiple R sessions via SSH.
#'
#' @param workers [\code{list} of \code{\link{Worker}}]\cr
#'   List of Workers as constructed with \code{\link{makeWorker}}.
#'
#' @note
#' If you use a custom \dQuote{.ssh/config} file, make sure your
#' ProxyCommand passes \sQuote{-q} to ssh, otherwise each output will
#' end with the message \dQuote{Killed by signal 1} and this will break
#' the communication with the nodes.
#'
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsSSH = function(workers) {
  force(workers)
  assertList(workers, types = "Worker")
  if (.Platform$OS.type == "windows")
    stop("clusterFunctionsSSH not compatible with Windows")
  nodenames = vcapply(workers, "[[", "nodename")
  if (anyDuplicated(nodenames))
    stop("Duplicated hostnames found in list of workers")
  names(workers) = nodenames
  rm(nodenames)

  submitJob = function(reg, jc) {
    rload = vnapply(workers, function(w) w$status$load / w$ncpus)
    worker = Find(function(w) w$available == "avail", sample(workers, prob = 1 / (rload + 0.1)), nomatch = NULL)

    if (!is.null(worker) && worker$available == "avail") {
      pid = try(startWorkerJob(worker, reg, jc$uri, jc$log.file))
      if (is.error(pid)) {
        makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = "Submit failed.")
      } else {
        worker$available = "unknown"
        makeSubmitJobResult(status = 0L, batch.id = sprintf("%s#%s", worker$nodename, pid))
      }
    } else {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = "Busy")
    }
  }

  killJob = function(reg, batch.id) {
    parts = stri_split_fixed(batch.id, "#")[[1L]]
    killWorkerJob(workers[[parts[1L]]], reg, parts[2L])
  }

  listJobs = function(reg) {
    unlist(lapply(workers, listWorkerJobs, reg = reg), use.names = FALSE)
  }

  makeClusterFunctions(name = "SSH", submitJob = submitJob, killJob = killJob,
    listJobs = listJobs, store.job = TRUE)
}
