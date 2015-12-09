asIds = function(reg, ids = NULL, n = NULL, default = NULL, extra.cols = FALSE) {
  if (is.null(ids)) {
    if (is.null(default))
      return(NULL)
    return(default)
  }

  if (is.data.frame(ids)) {
    assertInteger(ids$job.id, any.missing = FALSE, len = n)
    if (!is.data.table(ids))
      ids = as.data.table(ids)
    if (!identical(key(ids), "job.id"))
      setkeyv(ids, "job.id")
    if (!extra.cols && ncol(ids) > 1L)
      ids = ids[, "job.id", with = FALSE]
  } else {
    ids = data.table(job.id = asInteger(ids, any.missing = FALSE, len = n), key = "job.id")
  }

  i = ids[!reg$status, which = TRUE]
  if (length(i) > 0L)
    stopf("Illegal ids defined, e.g. %i", ids[i[1L]]$job.id)

  return(ids)
}

info = function(...) {
  if (getOption("batchtools.verbose", TRUE))
    message(sprintf(...))
}

now = function() {
  as.integer(Sys.time())
}

npath = function(file.dir, ...) {
  stri_replace_all_fixed(file.path(normalizePath(file.dir, winslash = "/", mustWork = FALSE), ...), "\\", "/")
}

getRandomString = function() {
  digest::digest(list(runif(1L), Sys.time()))
}

insert = function(x, y) {
  x[names2(y)] = y
  x
}

writeRDS = function(object, file, wait = FALSE, compress = getOption("batchtools.compress", TRUE)) {
  saveRDS(object, file = file, compress = compress)
  if (wait)
    while(!file.exists(file)) Sys.sleep(1)
  invisible(TRUE)
}

makeProgressBar = function(..., tokens = list()) {
  if (getOption("batchtools.verbose", TRUE) && getOption("batchtools.progress", TRUE)) {
    pb = progress_bar$new(..., show_after = 1)
    pb$tick(0L, tokens = tokens)
    return(pb)
  }
  list(tick = function(len = 1, tokens = list()) NULL, update = function(ratio, tokens) NULL)
}

seq_row = function(x) {
  seq_len(nrow(x))
}

vlapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA, USE.NAMES = use.names)
}

viapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_integer_, USE.NAMES = use.names)
}

vcapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_character_, USE.NAMES = use.names)
}

vnapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_real_, USE.NAMES = use.names)
}

is.error = function(x) {
  inherits(x, "try-error")
}

catf = function (..., con = stdout()) {
  cat(paste0(sprintf(...), collapse = "\n"), "\n", sep = "", file = con)
}

stopf = function (...) {
  stop(simpleError(sprintf(...), call = sys.call(sys.parent())))
}

warningf = function (...) {
  warning(simpleWarning(sprintf(...), call = sys.call(sys.parent())))
}

`%nin%` = function(x, y) {
  !match(x, y, nomatch = 0L)
}

names2 = function (x, missing.val = NA_character_) {
  n = names(x)
  if (is.null(n))
    return(rep.int(missing.val, length(x)))
  replace(n, is.na(n) | !nzchar(n), missing.val)
}

setClasses = function(x, cl) {
  setattr(x, "class", cl)
  x
}

suppressAll = function (expr) {
  invisible(capture.output({
    suppressWarnings(suppressMessages(suppressPackageStartupMessages(force(expr))))
  }))
}

droplevel = function(x, lvl) {
  levels(x) = replace(levels(x), levels(x) == lvl, NA_character_)
  x
}

map = function(f, ..., more.args = NULL) {
  f = match.fun(f)
  dots = list(...)
  .mapply(f, dots, more.args)
}
