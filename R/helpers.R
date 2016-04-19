asJobTable = function(reg, ids = NULL, default = NULL, keep.extra = FALSE) {
  if (is.null(ids) && !is.null(default))
    return(default)
  res = filter(reg$status, ids)[, "job.id", with = FALSE]
  if (keep.extra && is.data.frame(ids) && ncol(ids) >= 2L)
    res = cbind(res, ids[, !"job.id", with = FALSE])
  return(res)
}

filter = function(x, ids = NULL) {
  if (is.null(ids))
    return(x)
  if (identical(key(ids), "job.id"))
    return(x[unique(ids), nomatch = 0L])
  if (is.data.frame(ids))
    ids = ids$job.id
  if (qtest(ids, "X"))
    return(x[list(unique(as.integer(ids))), nomatch = 0L])
  stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
}

inner_join = function(x, y, on = key(y)) {
  x[y, nomatch = 0, on = on]
}

auto_increment = function(ids, n = 1L) {
  if (length(ids) == 0L) seq_len(n) else max(ids) + seq_len(n)
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

insert = function(x, y) {
  x[names2(y)] = y
  x[order(names2(x))]
}

writeRDS = function(object, file, wait = FALSE) {
  saveRDS(object, file = file)
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

capture = function(expr) {
  cleanup = function() {
    sink(type = "message")
    sink(type = "output")
    close(con)
  }

  output = character(0L)
  con = textConnection("output","w", local = TRUE)
  sink(file = con)
  sink(file = con, type = "message")
  on.exit(cleanup())
  res = try(eval(expr, parent.frame()))
  cleanup()
  on.exit(NULL)
  list(output = output, res = res)
}


filterNull = function(x) {
  x[!vlapply(x, is.null)]
}
