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
  ids = asIds(reg, ids, default = .findAll(reg))

  stats = getStatusSummary(ids, TRUE, reg = reg)
  setClasses(stats, c("Status", class(stats)))
}

getStatusSummary = function(ids, on.sys = FALSE, reg = getDefaultRegistry()) {
  stats = reg$status[ids][, list(
    defined   = .N,
    submitted = count(get("submitted")),
    started   = count(get("started")),
    done      = count(get("done")),
    error     = count(get("error"))
  )]
  stats$done = stats$done - stats$error
  if (on.sys)
    stats$on.sys = nrow(.findOnSystem(reg, ids))
  return(stats)
}

#' @export
print.Status = function(x, ...) {
  fmt = sprintf("%%-11s: %%%ii (%%5.1f%%%%)", nchar(x$defined))
  pr = function(label, h) catf(fmt, label, h, h / x$defined * 100)

  catf("Status for %i jobs:", x$defined)
  pr("  Submitted", x$submitted)
  pr("  Started", x$started)
  pr("  System", x$on.sys)
  pr("  Done", x$done)
  pr("  Error", x$error)
}
