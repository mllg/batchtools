#' @title ClusterFunctions for Remote SSH Execution
#'
#' @description
#' Jobs are spawned by starting multiple R sessions via SSH.
#'
#' @param workers [\code{list} of \code{\link{Worker}}]\cr
#'   List of Workers as constructed with \code{\link{Worker}}.
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
  assertList(workers, types = "Worker")
  if (testOS("windows"))
    stop("clusterFunctionsSSH not compatible with Windows")
  nodenames = vcapply(workers, "[[", "nodename")
  if (anyDuplicated(nodenames))
    stop("Duplicated hostnames found in list of workers")
  names(workers) = nodenames
  rm(nodenames)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    lapply(workers, function(w) w$update())
    rload = vnapply(workers, function(w) w$load / w$ncpus)
    worker = Find(function(w) w$status == "available", sample(workers, prob = 1 / (rload + 0.1)), nomatch = NULL)

    if (!is.null(worker) && worker$status == "available") {
      pid = try(worker$start(reg, jc$uri, jc$log.file))
      if (is.error(pid)) {
        makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = "Submit failed.")
      } else {
        makeSubmitJobResult(status = 0L, batch.id = sprintf("%s#%s", worker$nodename, pid))
      }
    } else {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = sprintf("Busy: %s", worker$status))
    }
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    parts = stri_split_fixed(batch.id, "#")[[1L]]
    worker = workers[[parts[1L]]]
    worker$kill(parts[2L])
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    unlist(lapply(workers, function(w) w$list(reg)), use.names = FALSE)
  }

  makeClusterFunctions(name = "SSH", submitJob = submitJob, killJob = killJob,
    listJobsRunning = listJobsRunning, store.job = TRUE)
}
