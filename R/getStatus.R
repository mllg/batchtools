#' @title Summarize the Computational Status
#'
#' @description
#' This function gives an encompassing overview over the computational status on your system.
#' The status can be one or many of the following:
#' \itemize{
#'  \item \dQuote{defined}: Jobs which are defined via \code{\link{batchMap}} or \code{\link{addExperiments}}, but are not yet submitted.
#'  \item \dQuote{submitted}: Jobs which are submitted to the batch system via \code{\link{submitJobs}}, scheduled for execution.
#'  \item \dQuote{started}: Jobs which have been started.
#'  \item \dQuote{done}: Jobs which terminated successfully.
#'  \item \dQuote{error}: Jobs which terminated with an exception.
#'  \item \dQuote{running}: Jobs which are listed by the cluster functions to be running on the live system. Not supported for all cluster functions.
#'  \item \dQuote{queued}: Jobs which are listed by the cluster functions to be queued on the live system. Not supported for all cluster functions.
#'  \item \dQuote{system}: Jobs which are listed by the cluster functions to be queued or running. Not supported for all cluster functions.
#'  \item \dQuote{expired}: Jobs which have been submitted, but vanished from the live system. Note that this is determined heuristically and may include some false positives.
#' }
#' Here, a job which terminated successfully counts towards the jobs which are submitted, started and done.
#' To retrieve the corresponding job ids, see \code{\link{findJobs}}.
#'
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{\link[data.table]{data.table}}] (with class \dQuote{Status} for printing).
#' @seealso \code{\link{findJobs}}
#' @export
#' @family debug
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' fun = function(i) if (i == 3) stop(i) else i
#' ids = batchMap(fun, i = 1:5, reg = tmp)
#' submitJobs(ids = 1:4, reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' tab = getStatus(reg = tmp)
#' print(tab)
#' str(tab)
getStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  stats = getStatusTable(convertIds(reg, ids), reg = reg)
  setClasses(stats, c("Status", class(stats)))
}

getStatusTable = function(ids = NULL, batch.ids = getBatchIds(reg = reg), reg = getDefaultRegistry()) {
  submitted = started = done = error = status = NULL
  
  stats = merge(filter(reg$status, ids), batch.ids, by = "batch.id", all.x = TRUE, all.y = FALSE, sort = FALSE)[,
    c("log.file", "timed.out") := list(
      getLogFiles(reg, job.id),
      Sys.time() > submitted + reg$cluster.functions$fs.latency
    )][, 
      log.file.exists := !is.na(log.file) & fs::file_exists(log.file)
    ][, list(
    defined       = .N,
    submitted     = count(submitted),
    started       = sum(!is.na(started) | !is.na(status) & status == "running"),
    done          = count(done),
    error         = count(error),
    queued        = sum(status == "queued", na.rm = TRUE),
    running       = sum(status == "running", na.rm = TRUE),
    provisioning  = sum(!is.na(submitted) & is.na(done) & is.na(status) & !log.file.exists & !is.na(log.file)),
    expired       = sum(!is.na(submitted) & is.na(done) & is.na(status) & (log.file.exists | (is.na(log.file) & timed.out)))
  )]
  stats$done = stats$done - stats$error
  stats$system = stats$queued + stats$provisioning + stats$running
  return(stats)
}

#' @export
print.Status = function(x, ...) {
  fmt = sprintf("  %%-13s: %%%ii (%%5.1f%%%%)", stri_width(x$defined))
  pr = function(label, h) catf(fmt, label, h, h / x$defined * 100)

  catf("Status for %i jobs at %s:", x$defined, strftime(Sys.time()))
  pr("Submitted", x$submitted)
  pr("-- Queued", x$queued)
  pr("-- Provisioning", x$provisioning)
  pr("-- Started", x$started)
  pr("---- Running", x$running)
  pr("---- Done", x$done)
  pr("---- Error", x$error)
  pr("---- Expired", x$expired)
}
