#' @title Wait for termination of jobs on the batch system
#'
#' @description
#' This function can be used to synchronise the execution on batch systems.
#'
#' @templateVar ids.default all
#' @template ids
#' @param sleep [\code{numeric(1)}]\cr
#'   Seconds to sleep between status updates. Default is \code{10}.
#' @param timeout [\code{numeric(1)}]\cr
#'   After waiting \code{timeout} seconds, show a message and return
#'   \code{FALSE}. This argument may be required on some systems where, e.g.,
#'   expired jobs or jobs on hold are problematic to detect. If you don't want
#'   a timeout, set this to \code{Inf}. Default is \code{604800} (one week).
#' @param stop.on.error [\code{logical(1)}]\cr
#'   Immediately cancel if a job terminates with an error? Default is
#'   \code{FALSE}.
#' @template reg
#' @return [\code{logical(1)}]. Returns \code{TRUE} if all jobs terminated
#' successfully and \code{FALSE} if either the timeout is reached or at least
#' one job terminated with an exception.
#' @export
waitForJobs = function(ids = NULL, sleep = 10, timeout = 604800, stop.on.error = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = FALSE)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findOnSystem(reg))
  assertNumeric(sleep, len = 1L, lower = 0.2, finite = TRUE)
  assertNumeric(timeout, len = 1L, lower = sleep)
  assertFlag(stop.on.error)

  # anything to do at all?
  cf = reg$cluster.functions
  n.jobs = nrow(ids)
  if (is.null(cf$listJobs) || n.jobs == 0L)
    return(TRUE)

  info("Waiting for %i jobs ...", n.jobs)
  timeout = now() + timeout

  pb = makeProgressBar(total = n.jobs, format = "Waiting [:bar] :percent eta: :eta")
  repeat {
    if (stop.on.error && nrow(.findError(ids = ids, reg = reg)) > 0L)
      return(FALSE)

    on.sys = .findOnSystem(ids = ids, reg = reg)
    if (nrow(on.sys) == 0L)
      return(nrow(.findNotDone(ids = ids, reg = reg)) == 0L)

    if (now() > timeout)
      return(FALSE)

    pb$tick(n.jobs - nrow(on.sys))
    Sys.sleep(sleep)
    suppressMessages(syncRegistry(reg = reg))
  }
}
