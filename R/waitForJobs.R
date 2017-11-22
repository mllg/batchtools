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
#' @param expire.after [\code{integer(1)}]\cr
#'   Jobs count as \dQuote{expired} if they are not found on the system but have not communicated back
#'   their results (or error message). This frequently happens on managed system if the scheduler kills
#'   a job because the job has hit the walltime or request more memory than reserved.
#'   On the other hand, network file systems often require several seconds for new files to be found,
#'   which can lead to false positives in the detection heuristic.
#'   \code{waitForJobs} treats such jobs as expired after they have not been detected on the system
#'   for \code{expire.after} iterations (default 3 iterations).
#' @param stop.on.error [\code{logical(1)}]\cr
#'   Immediately cancel if a job terminates with an error? Default is
#'   \code{FALSE}.
#' @param stop.on.expire [\code{logical(1)}]\cr
#'   Immediately cancel if jobs are detected to be expired? Default is \code{FALSE}.
#'   Expired jobs will then be ignored for the remainder of \code{waitForJobs()}.
#' @template reg
#' @return [\code{logical(1)}]. Returns \code{TRUE} if all jobs terminated
#'   successfully and \code{FALSE} if either the timeout is reached or at least
#'   one job terminated with an exception or expired.
#' @export
waitForJobs = function(ids = NULL, sleep = NULL, timeout = 604800, expire.after = 3L, stop.on.error = FALSE, stop.on.expire = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = FALSE, sync = TRUE)
  assertNumber(timeout, lower = 0)
  assertCount(expire.after, positive = TRUE)
  assertFlag(stop.on.error)
  assertFlag(stop.on.expire)
  sleep = getSleepFunction(reg, sleep)
  ids = convertIds(reg, ids, default = .findSubmitted(reg = reg))

  if (nrow(.findNotSubmitted(ids = ids, reg = reg)) > 0L) {
    warning("Cannot wait for unsubmitted jobs. Removing from ids.")
    ids = ids[.findSubmitted(ids = ids, reg = reg), nomatch = 0L]
  }

  if (nrow(ids) == 0L) {
    return(TRUE)
  }

  terminated = on.sys = expire.counter = NULL
  ids$terminated = FALSE
  ids$on.sys = FALSE
  ids$expire.counter = 0L

  timeout = Sys.time() + timeout
  pb = makeProgressBar(total = nrow(ids), format = "Waiting (Q::queued R::running D::done E::error ?::expired) [:bar] :percent eta: :eta")
  i = 0L

  repeat {
    ### case 1: all jobs terminated or expired -> nothing on system
    ids[.findTerminated(reg, ids), "terminated" := TRUE]
    if (ids[!(terminated) & expire.counter <= expire.after, .N] == 0L) {
      "!DEBUG [waitForJobs]: All jobs terminated"
      pb$update(1)
      waitForResults(reg, ids)
      return(nrow(.findDone(reg, ids)) == nrow(ids))
    }

    ### case 2: there are errors and stop.on.error is TRUE
    if (stop.on.error && nrow(.findErrors(reg, ids)) > 0L) {
      "!DEBUG [waitForJobs]: Errors found and stop.on.error is TRUE"
      pb$update(1)
      return(FALSE)
    }

    batch.ids = getBatchIds(reg)
    ids[, "on.sys" := FALSE][.findOnSystem(reg, ids, batch.ids = batch.ids), "on.sys" := TRUE]
    ids[(on.sys), "expire.counter" := 0L]
    ids[!(on.sys) & !(terminated), "expire.counter" := expire.counter + 1L]
    stats = getStatusTable(ids = ids, batch.ids = batch.ids, reg = reg)
    pb$update(mean(ids$terminated), tokens = as.list(stats))
    "!DEBUG [waitForJobs]: batch.ids: `stri_flatten(batch.ids$batch.id, ',')`"

    ### case 3: jobs disappeared, we cannot find them on the system after [expire.after] iterations
    if (stop.on.expire && ids[!(terminated) & expire.counter > expire.after, .N] > 0L) {
      warning("Jobs disappeared from the system")
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

