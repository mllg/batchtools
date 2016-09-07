#' @title Wait for Termination of Jobs
#'
#' @description
#' This function simply waits until all jobs are terminated.
#'
#' @templateVar ids.default findSubmitted
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
#'   successfully and \code{FALSE} if either the timeout is reached or at least
#'   one job terminated with an exception.
#' @export
waitForJobs = function(ids = NULL, sleep = 10, timeout = 604800, stop.on.error = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = FALSE, sync = TRUE)
  assertNumeric(sleep, len = 1L, lower = 0.2, finite = TRUE)
  assertNumeric(timeout, len = 1L, lower = sleep)
  assertFlag(stop.on.error)
  ids = convertIds(reg, ids, default = .findSubmitted(reg = reg))

  .findNotTerminated = function(reg, ids = NULL) {
    done = NULL
    filter(reg$status, ids, c("job.id", "done"))[is.na(done), "job.id", with = FALSE]
  }

  if (nrow(.findNotSubmitted(ids = ids, reg = reg)) > 0L) {
    warning("Cannot wait for unsubmitted jobs. Removing from ids.")
    ids = ids[.findSubmitted(ids = ids, reg = reg), nomatch = 0L]
  }

  n.jobs.total = n.jobs = nrow(ids)
  if (n.jobs == 0L)
    return(TRUE)

  batch.ids = getBatchIds(reg)
  if (nrow(batch.ids) == 0L)
    return(nrow(.findErrors(reg, ids)) == 0L)

  timeout = ustamp() + timeout
  ids.disappeared = noIds()

  pb = makeProgressBar(total = n.jobs.total, format = "Waiting (S::system R::running D::done E::error) [:bar] :percent eta: :eta",
    tokens = as.list(getStatusTable(ids, batch.ids, reg = reg)))

  repeat {
    # case 1: all jobs terminated -> nothing on system
    ids.nt = .findNotTerminated(reg, ids)
    if (nrow(ids.nt) == 0L) {
      pb$tick(n.jobs.total)
      return(nrow(.findErrors(reg, ids)) == 0L)
    }

    # case 2: there are errors and stop.on.error is TRUE
    if (stop.on.error && nrow(.findErrors(reg, ids)) > 0L) {
      pb$tick(n.jobs.total)
      return(FALSE)
    }

    # case 3: we have reached a timeout
    if (ustamp() > timeout) {
      pb$tick(n.jobs.total)
      warning("Timeout reached")
      return(FALSE)
    }

    # case 4: jobs disappeared, we cannot find them on the system
    # heuristic:
    #   job is not terminated, not on system and has not been on the system
    #   in the previous iteration
    ids.on.sys = .findOnSystem(reg, ids, batch.ids = batch.ids)
    if (nrow(ids.disappeared) > 0L) {
      if (nrow(ids.nt[!ids.on.sys, on = "job.id"][ids.disappeared, on = "job.id", nomatch = 0L]) > 0L) {
        warning("Some jobs disappeared from the system")
        pb$tick(n.jobs.total)
        return(FALSE)
      }
    }
    ids.disappeared = ids[!ids.on.sys, on = "job.id"]

    stats = getStatusTable(ids = ids, batch.ids = batch.ids, reg = reg)
    pb$tick(n.jobs - nrow(ids.nt), tokens = as.list(stats))
    n.jobs = nrow(ids.nt) # FIXME: remove after progress is updated

    Sys.sleep(sleep)
    suppressMessages(syncRegistry(reg = reg))
    batch.ids = getBatchIds(reg)
  }
}
