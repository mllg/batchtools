### FIXME: refactor
asJobTable = function(reg, ids = NULL, default = NULL, keep.extra = FALSE, single.id = FALSE) {
  if (is.null(ids) && !is.null(default))
    return(default)

  res = filter(reg$status, ids)[, "job.id", with = FALSE]
  if (single.id && nrow(res) != 1L)
    stopf("You must provide exactly one id (%i provided)", nrow(res))
  if (keep.extra && is.data.frame(ids) && ncol(ids) >= 2L)
    res = merge(res, ids, by = "job.id", all = TRUE)
  return(res)
}

filter = function(x, ids = NULL) {
  if (is.null(ids))
    return(copy(x))

  if (is.data.frame(ids) && "job.id" %in% names(ids)) {
    if (!is.data.table(ids))
      ids = as.data.table(ids)
  } else if (qtest(ids, "X")) {
    ids = data.table(job.id = as.integer(ids), key = "job.id")
  } else {
    stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
  }

  setkeyv(x[unique(ids, by = "job.id"), nomatch = 0L], "job.id")
}

inner_join = function(x, y) {
  x[y, nomatch = 0, on = key(x)]
}

auto_increment = function(ids, n = 1L) {
  if (length(ids) == 0L) seq_len(n) else max(ids) + seq_len(n)
}

ustamp = function() {
  as.integer(Sys.time())
}

now = function() {
  strftime(Sys.time())
}

npath = function(file.dir, ...) {
  normalizePath(file.path(file.dir, ...), winslash = "/", mustWork = FALSE)
}

insert = function(x, y) {
  x[names2(y)] = y
  x[order(names2(x))]
}

writeRDS = function(object, file, wait = FALSE) {
  saveRDS(object, file = file)
  if (wait)
    while(!file.exists(file)) Sys.sleep(0.5)
  invisible(TRUE)
}

makeProgressBar = function(..., tokens = list()) {
  if (getOption("batchtools.verbose", TRUE) && getOption("batchtools.progress", TRUE) && getOption("width") >= 20L) {
    pb = progress_bar$new(..., show_after = 1, width = getOption("width") - 5L)
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

vnapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_real_, USE.NAMES = use.names)
}

vcapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_character_, USE.NAMES = use.names)
}

is.error = function(x) {
  inherits(x, "try-error")
}

# formating message
info = function(...) {
  if (getOption("batchtools.verbose", TRUE))
    message(sprintf(...))
}

# concatenating cat()
catc = function (..., con = stdout()) {
  cat(stri_join(..., collapse = "\n"), "\n", sep = "", file = con)
}

# formating cat()
catf = function (..., con = stdout()) {
  cat(stri_join(sprintf(...), collapse = "\n"), "\n", sep = "", file = con)
}

# formating waring()
warningf = function (...) {
  warning(simpleWarning(sprintf(...), call = sys.call(sys.parent())))
}

# formating stop()
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

stri_trunc = function(str, length, append = "") {
  if (is.na(str))
    return(str)
  if (stri_length(str) > length) {
    if (is.na(append) || !nzchar(append))
      return(stri_sub(str, 1L, length))
    return(stri_join(stri_sub(str, 1L, length - stri_length(append)), append))
  }
  return(str)
}

Rscript = function() {
  file.path(R.home("bin"), ifelse(testOS("windows"), "Rscript.exe", "Rscript"))
}

findConfFile = function() {
  uris = file.path(c(".", "~"), c("batchtools.conf.R", ".batchtools.conf.R"))
  i = wf(file.exists(uris))
  return(npath(uris[i]))
}

findTemplateFile = function(name) {
  uris = c(
    sprintf("batchtools.%s.tmpl", name),
    file.path("~", sprintf(".batchtools.%s.tmpl", name)),
    system.file("templates", sprintf("%s.default.tmpl", name), package = "batchtools")
  )
  i = wf(nzchar(uris) & file.exists(uris))
  return(npath(uris[i]))
}
