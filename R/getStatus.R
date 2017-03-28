#' @title Summarize the Computational Status
#'
#' @description
#' This function gives an encompassing overview over the computational status on your system.
#'
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{\link[data.table]{data.table}}] (with class \dQuote{Status} for printing).
#' @export
#' @family debug
#' @examples
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
  submitted = started = done = error = batch.id = status = NULL
  stats = filter(reg$status, ids)[, list(
    defined   = .N,
    submitted = count(submitted),
    started   = sum(!is.na(started) | batch.id %chin% batch.ids[status == "running"]$batch.id),
    done      = count(done),
    error     = count(error),
    queued    = sum(batch.id %chin% batch.ids[status == "queued"]$batch.id),
    running   = sum(batch.id %chin% batch.ids[status == "running"]$batch.id),
    expired   = sum(!is.na(submitted) & is.na(done) & batch.id %nin% batch.ids$batch.id)
  )]
  stats$done = stats$done - stats$error
  stats$system = stats$queued + stats$running
  return(stats)
}

#' @export
print.Status = function(x, ...) {
  fmt = sprintf("  %%-10s: %%%ii (%%5.1f%%%%)", stri_width(x$defined))
  pr = function(label, h) catf(fmt, label, h, h / x$defined * 100)

  catf("Status for %i jobs:", x$defined)
  pr("Submitted", x$submitted)
  pr("Queued", x$queued)
  pr("Started", x$started)
  pr("Running", x$running)
  pr("Done", x$done)
  pr("Error", x$error)
  pr("Expired", x$expired)
}
