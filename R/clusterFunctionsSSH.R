#' @title ClusterFunctions for Remote SSH Execution
#'
#' @description
#' Jobs are spawned by starting multiple R sessions via \code{Rscript} over SSH.
#' If the hostname of the \code{\link{Worker}} equals \dQuote{localhost},
#' \code{Rscript} is called directly so that you do not need to have an SSH client installed.
#'
#' @param workers [\code{list} of \code{\link{Worker}}]\cr
#'   List of Workers as constructed with \code{\link{Worker}}.
#' @inheritParams makeClusterFunctions
#'
#' @note
#' If you use a custom \dQuote{.ssh/config} file, make sure your
#' ProxyCommand passes \sQuote{-q} to ssh, otherwise each output will
#' end with the message \dQuote{Killed by signal 1} and this will break
#' the communication with the nodes.
#'
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
#' @examples
#' \dontrun{
#' # cluster functions for multicore execution on the local machine
#' makeClusterFunctionsSSH(list(Worker$new("localhost", ncpus = 2)))
#' }
makeClusterFunctionsSSH = function(workers, fs.latency = 65) { # nocov start
  assertList(workers, types = "Worker")
  names(workers) = vcapply(workers, "[[", "nodename")
  if (anyDuplicated(names(workers)))
    stop("Duplicated hostnames found in list of workers")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    lapply(workers, function(w) w$update(reg))
    rload = vnapply(workers, function(w) w$max.load / w$ncpus)
    worker = Find(function(w) w$status == "available", sample(workers, prob = 1 / (rload + 0.1)), nomatch = NULL)

    if (!is.null(worker) && worker$status == "available") {
      pid = try(worker$start(reg, jc$uri, jc$log.file))
      if (is.error(pid)) {
        makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = "Submit failed.")
      } else {
        makeSubmitJobResult(status = 0L, batch.id = sprintf("%s#%s", worker$nodename, pid$output))
      }
    } else {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = sprintf("Busy: %s", workers[[1L]]$status))
    }
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    nodename = stri_split_fixed(batch.id, "#", n = 2L)[[1L]][1L]
    workers[[nodename]]$kill(reg, batch.id)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    unlist(lapply(workers, function(w) w$list(reg)), use.names = FALSE)
  }

  makeClusterFunctions(name = "SSH", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    store.job.collection = TRUE, fs.latency = fs.latency)
} # nocov end
