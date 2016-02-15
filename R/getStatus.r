#' @title Summarize the Computational Status
#'
#' @description
#' This function gives an encompassing overview over the computational status
#' on your system.
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{\link[data.table]{data.table}}] with class \dQuote{Status} to
#'   trigger a nice print function.
#' @export
getStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)

  stats = .getStatus(ids, reg = reg)
  setClasses(stats, c("Status", class(stats)))
}

.getStatus = function(ids, batch.ids = getBatchIds(reg = reg), reg = getDefaultRegistry()) {
  stats = filter(reg$status, ids)[, list(
    defined   = .N,
    submitted = count(submitted),
    started   = count(started),
    done      = count(done),
    error     = count(error),
    queued    = sum(batch.id %in% batch.ids[status == "queued"]$batch.id),
    running   = sum(batch.id %in% batch.ids[status == "running"]$batch.id)
  )]
  stats$done = stats$done - stats$error
  stats$on.sys = stats$queued + stats$running
  return(stats)
}

#' @export
print.Status = function(x, ...) {
  fmt = sprintf("%%-11s: %%%ii (%%5.1f%%%%)", nchar(x$defined))
  pr = function(label, h) catf(fmt, label, h, h / x$defined * 100)

  catf("Status for %i jobs:", x$defined)
  pr("  Submitted", x$submitted)
  pr("  Queued", x$queued)
  pr("  Started", x$started)
  pr("  Running", x$running)
  pr("  Done", x$done)
  pr("  Error", x$error)
}
