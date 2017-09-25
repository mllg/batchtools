#' @title ClusterFunctions for Sequential Execution in the Running R Session
#'
#' @description
#' All jobs are executed sequentially using the current R process in which \code{\link{submitJobs}} is called.
#' Thus, \code{submitJob} blocks the session until the job has finished.
#' The main use of this \code{ClusterFunctions} implementation is to test and debug programs on a local computer.
#'
#' Listing jobs returns an empty vector (as no jobs can be running when you call this)
#' and \code{killJob} is not implemented for the same reasons.
#'
#' @param external [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, jobs are started in a fresh R session instead of currently active but still
#'   waits for its termination.
#'   Default is \code{FALSE}.
#' @param write.logs [\code{logical(1)}]\cr
#'   Sink the output to log files. Turning logging off can increase the speed of
#'   calculations but makes it very difficult to debug.
#'   Default is \code{TRUE}.
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsInteractive = function(external = FALSE, write.logs = TRUE, fs.latency = NA_real_) {
  assertFlag(external)
  assertFlag(write.logs)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    if (external) {
      runOSCommand(Rscript(), sprintf("-e \"batchtools::doJobCollection('%s', output = '%s')\"", jc$uri, jc$log.file))
      makeSubmitJobResult(status = 0L, batch.id = "cfInteractive")
    } else {
      doJobCollection(jc, output = jc$log.file)
      makeSubmitJobResult(status = 0L, batch.id = "cfInteractive")
    }
  }

  makeClusterFunctions(name = "Interactive", submitJob = submitJob, store.job.collection = external, fs.latency = fs.latency)
}
