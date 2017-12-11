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
  round(as.numeric(Sys.time()), 4L)
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

file.remove.safely = function(x) {
  file.remove(x[file.exists(x)])

  while(any(i <- file.exists(x))) {
    Sys.sleep(0.5)
    file.remove(x[i])
  }
}

writeRDS = function(object, file) {
  file.remove.safely(file)
  saveRDS(object, file = file)
  waitForFile(file, 300)
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

`%chnin%` = function(x, y) {
  !chmatch(x, y, nomatch = 0L)
}

setClasses = function(x, cl) {
  setattr(x, "class", cl)
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
  fp(R.home("bin"), ifelse(testOS("windows"), "Rscript.exe", "Rscript"))
}

getSeed = function(start.seed, id) {
  if (id > .Machine$integer.max - start.seed)
    start.seed - .Machine$integer.max + id
  else
    start.seed + id
}

chsetdiff = function(x, y) {
  # Note: assumes that x has no duplicates
  x[chmatch(x, y, 0L) == 0L]
}

chintersect = function(x, y) {
  # Note: assumes that x has no duplicates
  x[chmatch(y, x, 0L)]
}

rnd_hash = function(prefix = "") {
  stri_join(prefix, digest(list(runif(1L), as.numeric(Sys.time()))))
}
