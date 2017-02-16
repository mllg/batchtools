mergedJobs = function(reg, ids, cols) {
  if (is.null(ids))
    reg$defs[reg$status, cols, on = "def.id", nomatch = 0L, with = missing(cols)]
  else
    reg$defs[reg$status[ids, nomatch = 0L, on = "job.id"], cols, on = "def.id", nomatch = 0L, with = missing(cols)]
}

auto_increment = function(ids, n = 1L) {
  if (length(ids) == 0L) seq_len(n) else max(ids) + seq_len(n)
}

ustamp = function() {
  round(as.numeric(Sys.time(), 4L))
}

npath = function(path, must.work = TRUE) {
  if (stri_startswith_fixed(path, "~")) {
    # do not call normalizePath, we do not want to expand this paths relative to home
    if (must.work && !file.exists(path))
      stopf("File '%s' not found", path)
    if (testOS("windows"))
      path = stri_replace_all_fixed(path, "\\", "/")
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = must.work)
}

names2 = function (x, missing.val = NA_character_) {
  n = names(x)
  if (is.null(n))
    return(rep.int(missing.val, length(x)))
  replace(n, is.na(n) | !nzchar(n), missing.val)
}

insert = function(x, y) {
  x[names2(y)] = y
  x[order(names2(x))]
}

writeRDS = function(object, file) {
  if (file.exists(file))
    file.remove(file)
  saveRDS(object, file = file)
  while(!file.exists(file)) Sys.sleep(0.5)
  invisible(TRUE)
}

makeProgressBar = function(...) {
  if (!batchtools$debug && getOption("batchtools.verbose", TRUE) && getOption("batchtools.progress", TRUE) && getOption("width") >= 20L) {
    progress_bar$new(...)
  } else {
    list(tick = function(len = 1, tokens = list()) NULL, update = function(ratio, tokens) NULL)
  }
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

vnapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_real_, USE.NAMES = use.names)
}

vcapply = function (x, fun, ..., use.names = TRUE) {
  vapply(X = x, FUN = fun, ..., FUN.VALUE = NA_character_, USE.NAMES = use.names)
}

is.error = function(x) {
  inherits(x, "try-error")
}

# formating info message
info = function(...) {
  if (getOption("batchtools.verbose", TRUE))
    message(sprintf(...))
}

# formating cat()
catf = function (..., con = "") {
  cat(stri_flatten(sprintf(...), "\n"), "\n", sep = "", file = con)
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

setClasses = function(x, cl) {
  setattr(x, "class", cl)
  x
}

addlevel = function(x, lvl) {
  if (lvl %nin% levels(x))
    levels(x) = c(levels(x), lvl)
  x
}

rmlevel = function(x, lvl) {
  levels(x) = replace(levels(x), levels(x) == lvl, NA_character_)
  x
}

#' @useDynLib batchtools count_not_missing
count = function(x) {
  .Call(count_not_missing, x)
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

getSeed = function(start.seed, id) {
  if (id > .Machine$integer.max - start.seed)
    start.seed - .Machine$integer.max + id
  else
    start.seed + id
}

with_seed = function(seed, expr) {
  if (!is.null(seed)) {
    if (!exists(".Random.seed", .GlobalEnv))
      set.seed(NULL)
    state = get(".Random.seed", .GlobalEnv)
    set.seed(seed)
    on.exit(assign(".Random.seed", state, envir = .GlobalEnv))
  }
  eval.parent(expr)
}

chsetdiff = function(x, y) {
  # Note: assumes that x has no duplicates
  x[chmatch(x, y, 0L) == 0L]
}

chintersect = function(x, y) {
  # Note: assumes that x has no duplicates
  x[chmatch(y, x, 0L)]
}
