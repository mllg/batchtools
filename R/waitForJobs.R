#' @title Wait for Termination of Jobs
#'
#' @description
#' This function simply waits until all jobs are terminated.
#'
#' @templateVar ids.default findSubmitted
#' @template ids
#' @param sleep [\code{function(i)} | \code{numeric(1)}]\cr
#'   Parameter to control the duration to sleep between queries.
#'   You can pass an absolute numeric value in seconds or a \code{function(i)} which returns
#'   the number of seconds to sleep in the \code{i}-th iteration.
#'   If not provided (\code{NULL}), tries to read the value (number/function) from the configuration file
#'   (stored in \code{reg$sleep}) or defaults to a function with exponential backoff between
#'   5 and 120 seconds.
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
waitForJobs = function(ids = NULL, sleep = NULL, timeout = 604800, stop.on.error = FALSE, reg = getDefaultRegistry()) {
  expire.after = 3L
  assertRegistry(reg, writeable = FALSE, sync = TRUE)
  assertNumber(timeout, lower = 0)
  assertFlag(stop.on.error)
  sleep = getSleepFunction(reg, sleep)
  ids = convertIds(reg, ids, default = .findSubmitted(reg = reg))


  if (nrow(.findNotSubmitted(ids = ids, reg = reg)) > 0L) {
    warning("Cannot wait for unsubmitted jobs. Removing from ids.")
    ids = ids[.findSubmitted(ids = ids, reg = reg), nomatch = 0L]
  }

  if (nrow(ids) == 0L)
    return(TRUE)

  ids$terminated = FALSE
  ids$expire.counter = 0L

  timeout = Sys.time() + timeout
  pb = makeProgressBar(total = nrow(ids), format = "Waiting (S::system R::running D::done E::error) [:bar] :percent eta: :eta")
  i = 0L

  repeat {
    ### case 1: all jobs terminated -> nothing on system
    ids[.findTerminated(reg, ids), ("terminated") := TRUE]
    if (ids[!(terminated), .N] == 0L) {
      "!DEBUG [waitForJobs]: All jobs terminated"
      pb$update(1)
      waitForResults(reg, ids)
      return(nrow(.findErrors(reg, ids)) == 0L)
    }

    ### case 2: there are errors and stop.on.error is TRUE
    if (stop.on.error && nrow(.findErrors(reg, ids)) > 0L) {
      "!DEBUG [waitForJobs]: Errors found and stop.on.error is TRUE"
      pb$update(1)
      return(FALSE)
    }

    batch.ids = getBatchIds(reg)
    ids$on.sys = ids$job.id %in% .findOnSystem(reg, ids, batch.ids = batch.ids)
    ids[!(on.sys) & !(terminated), expire.counter := expire.counter + 1L]
    stats = getStatusTable(ids = ids, batch.ids = batch.ids, reg = reg)
    pb$update(mean(ids$terminated), tokens = as.list(stats))
    "!DEBUG [waitForJobs]: batch.ids: `stri_flatten(batch.ids$batch.id, ',')`"

    ### case 3: jobs disappeared, we cannot find them on the system in [expire.after] iterations
    if (ids[!(terminated) & expire.counter > expire.after, .N] > 0L) {
      warning("Some jobs disappeared from the system")
      pb$update(1)
      waitForResults(reg, ids)
      return(FALSE)
    }

    # case 4: we reach a timeout
    sleep(i)
    i = i + 1L
    if (Sys.time() > timeout) {
      pb$update(1)
      warning("Timeout reached")
      return(FALSE)
    }

    if (suppressMessages(sync(reg = reg)))
      saveRegistry(reg)
  }
}

.findNotTerminated = function(reg, ids = NULL) {
  done = NULL
  filter(reg$status, ids, c("job.id", "done"))[is.na(done), "job.id"]
}

.findTerminated = function(reg, ids = NULL) {
  done = NULL
  filter(reg$status, ids, c("job.id", "done"))[!is.na(done), "job.id"]
}
