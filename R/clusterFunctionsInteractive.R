#' @title ClusterFunctions for Sequential Execution in the Running R Session
#'
#' @description
#' All jobs executed under these cluster functions are executed
#' sequentially, in the same interactive R process that you currently are.
#' That is, \code{submitJob} does not return until the
#' job has finished. The main use of this \code{ClusterFunctions}
#' implementation is to test and debug programs on a local computer.
#'
#' Listing jobs returns an empty vector (as no jobs can be running when you call this)
#' and \code{killJob} returns at once (for the same reason).
#'
#' @param external [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, jobs are started in a fresh R session instead of currently active.
#'   Default is \code{FALSE}.
#' @param write.logs [\code{logical(1)}]\cr
#'   Sink the output to log files. Turning logging off can increase the speed of
#'   calculations but makes it very difficult to debug.
#'   Default is \code{TRUE}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsInteractive = function(external = FALSE, write.logs = TRUE) {
  assertFlag(external)
  assertFlag(write.logs)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    if (external) {
      runOSCommand(Rscript(), sprintf("-e \"batchtools::doJobCollection('%s', output = '%s')\"", jc$uri, jc$log.file))
      makeSubmitJobResult(status = 0L, batch.id = "cfInteractive", msg = "")
    } else {
      doJobCollection(jc, output = jc$log.file)
      makeSubmitJobResult(status = 0L, batch.id = "cfInteractive", msg = "")
    }
  }

  makeClusterFunctions(name = "Interactive", submitJob = submitJob, store.job = external)
}
