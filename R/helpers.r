filter = function(x, ids = NULL) {
  if (is.null(ids))
    return(x)
  if (is.data.frame(ids)) {
    w = x[ids, on = key(x), nomatch = 0L, which = TRUE]
  } else if (qtest(ids, "x")) {
    w = x[list(as.integer(ids)), nomatch = 0L, which = TRUE]
  } else {
    stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
  }
  x[unique(w)]
}

inner_join = function(x, y, on = key(y)) {
  x[y, nomatch = 0, on = on]
}

asJobIds = function(reg, ids = NULL, default = NULL, keep.extra = FALSE) {
  if (is.null(ids) && !is.null(default))
    return(default)
  res = filter(reg$status, ids)[, "job.id", with = FALSE]
  if (keep.extra && is.data.frame(ids)) inner_join(res, ids) else res
}

assertJobIds = function(ids, empty.ok = TRUE, single.id = FALSE) {
  if (!empty.ok && nrow(ids) == 0L)
    stop("You must provide at least 1 id")
  if (single.id && nrow(ids) != 1L)
    stopf("You must provide exactly 1 id (%i provided)", nrow(ids))
  ids
}

now = function() {
  as.integer(Sys.time())
}

stamp = function() {
  strftime(Sys.time())
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

info = function(...) {
  if (getOption("batchtools.verbose", TRUE))
    message(sprintf(...))
}

catf = function (..., con = stdout()) {
  cat(stri_join(sprintf(...), collapse = "\n"), "\n", sep = "", file = con)
}

warningf = function (...) {
  warning(simpleWarning(sprintf(...), call = sys.call(sys.parent())))
}

stopf = function (...) {
  stop(simpleError(sprintf(...), call = sys.call(sys.parent())))
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
    suppressWarnings(suppressMessages(suppressPackageStartupMessages(x <- force(expr))))
  }))
  return(x)
}

addlevel = function(x, lvl) {
  if (lvl %nin% levels(x))
    levels(x) = c(levels(x), lvl)
  x
}

droplevel = function(x, lvl) {
  levels(x) = replace(levels(x), levels(x) == lvl, NA_character_)
  x
}

#' @useDynLib batchtools count_not_missing
count = function(x) {
  .Call(count_not_missing, x)
}
