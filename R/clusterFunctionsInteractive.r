#' Create cluster functions for sequential execution in same session.
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
#' @param write.logs [\code{logical(1)}]\cr
#'   Sink the output to log files. Turning logging off can increase the speed of
#'   calculations but makes it very difficult to debug.
#'   Default is \code{TRUE}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsInteractive = function(write.logs = TRUE) {
  assertFlag(write.logs)

  submitJob = function(reg, jc) {
    if (write.logs) {
      fp = file(jc$log.file, open = "wt")
      on.exit(close(fp))
    } else {
      fp = stdout()
    }

    # sink both output and message streams
    doJobs(jc, con = fp)

    # return job result (always successful)
    makeSubmitJobResult(status = 0L, batch.id = "cfInteractive", msg = "")
  }

  makeClusterFunctions(name = "Interactive", submitJob = submitJob, store.job = FALSE)
}
