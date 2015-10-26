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
#' @useDynLib batchtools count_not_missing
#' @export
getStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  count = function(x) {
    .Call("count_not_missing", x, PACKAGE = "batchtools")
  }
  assertRegistry(reg)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findAll(reg))

  stats = reg$status[ids][, list(
    defined   = .N,
    submitted = count(get("submitted")),
    started   = count(get("started")),
    done      = count(get("done")),
    error     = count(get("error"))
  )]
  stats$done = stats$done - stats$error
  stats$on.sys = nrow(.findOnSystem(reg, ids))

  setClasses(stats, c("Status", class(stats)))
}

#' @export
print.Status = function(x, ...) {
  fmt = sprintf("%%-9s: %%%ii (%%5.1f%%%%)", nchar(x$defined))
  pr = function(label, h) catf(fmt, label, h, h / x$defined * 100)

  catf("Status for %i jobs:", x$defined)
  pr("  Submitted", x$submitted)
  pr("  Started", x$started)
  pr("  System", x$on.sys)
  pr("  Done", x$done)
  pr("  Error", x$error)
}
