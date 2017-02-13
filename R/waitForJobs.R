#' @title Wait for Termination of Jobs
#'
#' @description
#' This function simply waits until all jobs are terminated.
#'
#' @templateVar ids.default findSubmitted
#' @template ids
#' @param sleep [\code{function(i)} | \code{numeric(1)}]\cr
#'   Function which returns the duration to sleep in the \code{i}-th iteration.
#'   Alternatively, you can pass a single positive numeric value.
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
waitForJobs = function(ids = NULL, sleep = default.sleep, timeout = 604800, stop.on.error = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = FALSE, sync = TRUE)
  assertNumber(timeout, lower = 0)
  assertFlag(stop.on.error)
  sleep = getSleepFunction(sleep)
  ids = convertIds(reg, ids, default = .findSubmitted(reg = reg))

  .findNotTerminated = function(reg, ids = NULL) {
    done = NULL
    filter(reg$status, ids, c("job.id", "done"))[is.na(done), "job.id"]
  }

  if (nrow(.findNotSubmitted(ids = ids, reg = reg)) > 0L) {
    warning("Cannot wait for unsubmitted jobs. Removing from ids.")
    ids = ids[.findSubmitted(ids = ids, reg = reg), nomatch = 0L]
  }

  n.jobs = nrow(ids)
  if (n.jobs == 0L)
    return(TRUE)

  batch.ids = getBatchIds(reg)
  "!DEBUG [waitForJobs]: Using `nrow(ids)` ids and `nrow(batch.ids)` initial batch ids"

  timeout = Sys.time() + timeout
  ids.disappeared = noIds()
  pb = makeProgressBar(total = n.jobs, format = "Waiting (S::system R::running D::done E::error) [:bar] :percent eta: :eta")
  i = 1L

  repeat {
    # case 1: all jobs terminated -> nothing on system
    ids.nt = .findNotTerminated(reg, ids)
    if (nrow(ids.nt) == 0L) {
      "!DEBUG [waitForJobs]: All jobs terminated"
      pb$update(1)
      waitForResults(reg, ids)
      return(nrow(.findErrors(reg, ids)) == 0L)
    }

    stats = getStatusTable(ids = ids, batch.ids = batch.ids, reg = reg)
    pb$update((n.jobs - nrow(ids.nt)) / n.jobs, tokens = as.list(stats))

    # case 2: there are errors and stop.on.error is TRUE
    if (stop.on.error && nrow(.findErrors(reg, ids)) > 0L) {
      "!DEBUG [waitForJobs]: Errors found and stop.on.error is TRUE"
      pb$update(1)
      return(FALSE)
    }

    # case 3: we have reached a timeout
    if (Sys.time() > timeout) {
      pb$update(1)
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
        pb$update(1)
        waitForResults(reg, ids)
        return(FALSE)
      }
    }

    ids.disappeared = ids[!ids.on.sys, on = "job.id"]
    "!DEBUG [waitForJobs]: `nrow(ids.disappeared)` jobs disappeared"

    sleep(i)
    i = 1 + 1L
    suppressMessages(syncRegistry(reg = reg))
    batch.ids = getBatchIds(reg)
    "!DEBUG [waitForJobs]: New batch.ids: `stri_flatten(batch.ids$batch.id, ',')`"
  }
}
