#' @title Wait for Termination of Jobs
#'
#' @description
#' This function can be used to synchronize the execution on batch systems.
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
#' successfully and \code{FALSE} if either the timeout is reached or at least
#' one job terminated with an exception.
#' @export
waitForJobs = function(ids = NULL, sleep = 10, timeout = 604800, stop.on.error = FALSE, reg = getDefaultRegistry()) {
  # .findTerminated = function(reg, ids = NULL) {
  #   done = NULL
  #   reg$status[ids][!is.na(done), "job.id", with = FALSE]
  # }
  .findNotTerminated = function(reg, ids = NULL) {
    done = NULL
    reg$status[ids][is.na(done), "job.id", with = FALSE]
  }

  assertRegistry(reg, writeable = FALSE)
  assertNumeric(sleep, len = 1L, lower = 0.2, finite = TRUE)
  assertNumeric(timeout, len = 1L, lower = sleep)
  assertFlag(stop.on.error)

  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findSubmitted(reg = reg))
  cf = reg$cluster.functions
  n.jobs.total = n.jobs = nrow(ids)

  if (nrow(.findNotSubmitted(ids = ids, reg = reg)) > 0L) {
    warning("Cannot wait for unsubmitted jobs. Removing from ids.")
    ids = ids[.findSubmitted(ids = ids, reg = reg), nomatch = 0L]
  }

  if (n.jobs == 0L || nrow(ids) == 0L) {
    return(TRUE)
  }

  if (is.null(cf$listJobs)) {
    return(nrow(.findError(ids = ids, reg = reg)) == 0L)
  }

  info("Waiting for %i jobs ...", n.jobs)
  timeout = now() + timeout

  pb = makeProgressBar(total = n.jobs, format = "Waiting (R::running D::done E::error) [:bar] :percent eta: :eta",
    tokens = list(running = "?", done = "?", error = "?"))
  ids.disappeared = data.table(job.id = integer(0L), key = "job.id")

  repeat {
    # case 1: all are terminated -> nothing on system
    ids.nt = .findNotTerminated(ids = ids, reg = reg)
    if (nrow(ids.nt) == 0L) {
      pb$tick(n.jobs.total)
      return(nrow(.findError(ids = ids, reg = reg)) == 0L)
    }

    # case 2: there are errors and stop.on.error is set
    if (stop.on.error && nrow(.findError(ids = ids, reg = reg)) > 0L) {
      pb$tick(n.jobs.total)
      return(FALSE)
    }

    # case 3: we have reached a timeout
    if (now() > timeout) {
      pb$tick(n.jobs.total)
      warning("Timeout reached")
      return(FALSE)
    }

    # case 4: jobs disappeared, we cannot find them on the system
    # heuristic:
    #   job is not terminated, not on system and has not been on the system
    #   in the previous iteration
    ids.on.sys = .findOnSystem(ids = ids.nt, reg = reg)
    if (nrow(ids.disappeared) > 0L) {
      if (nrow(ids.nt[!ids.on.sys][ids.disappeared, nomatch = 0L]) > 0L) {
        warning("Some jobs disappeared from the system")
        pb$tick(n.jobs.total)
        return(FALSE)
      }
    } else {
      ids.disappeared = ids[!ids.on.sys]
    }

    stats = as.list(getStatusSummary(ids, FALSE, reg = reg)[, c("done", "error"), with = FALSE])
    stats$running = nrow(ids.on.sys)
    pb$tick(n.jobs - nrow(ids.nt), tokens = stats)
    n.jobs = nrow(ids.nt) # FIXME: remover after progress is updated

    Sys.sleep(sleep)
    suppressMessages(syncRegistry(reg = reg))
  }
}
