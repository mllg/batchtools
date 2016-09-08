allIds = function(reg) {
  reg$status[, "job.id", with = FALSE]
}

noIds = function() {
  data.table(job.id = integer(0L), key = "job.id")
}

castIds = function(ids, setkey = TRUE, ensure.copy = FALSE) {
  if (is.data.table(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")
    if (setkey && !identical(key(ids), "job.id")) {
      ids = copy(ids)
      setkeyv(ids, "job.id")
    } else if (ensure.copy) {
      ids = copy(ids)
    }
  } else if (is.data.frame(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")
    ids = as.data.table(ids)
    if (setkey)
      setkeyv(ids, "job.id")
  } else if (qtest(ids, "X")) {
    ids = if (setkey) data.table(job.id = as.integer(ids), key = "job.id") else data.table(job.id = as.integer(ids))
  } else {
    stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
  }

  return(ids)
}

convertIds = function(reg, ids, default = NULL, keep.extra = FALSE, keep.order = FALSE) {
  if (is.null(ids))
    return(default)

  ids = castIds(ids, setkey = !keep.order)
  if (anyDuplicated(ids, by = "job.id"))
    stop("Duplicated ids provided")

  if (!identical(keep.extra, FALSE) && ncol(ids) > 1L) {
    i = ids[reg$status, on = "job.id", nomatch = 0L, which = TRUE]
    if (isTRUE(keep.extra))
      return(ids[i])
    keep.extra = intersect(keep.extra, names(ids))
    return(ids[i, keep.extra, with = FALSE])
  }
  return(reg$status[ids, "job.id", on = "job.id", nomatch = 0L, with = FALSE])
}

convertId = function(reg, id) {
  id = convertIds(reg, id)
  if (nrow(id) != 1L)
    stopf("You must provide exactly one id (%i provided)", nrow(id))
  return(id)
}

filter = function(x, y, cols) {
  if (is.null(y))
    return(x[, cols, with = missing(cols)])
  return(x[y, cols, on = key(x), nomatch = 0L, with = missing(cols)])
}

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
  as.integer(Sys.time())
}

npath = function(path, normalize = stri_startswith_fixed(path, "~"), must.work = TRUE) {
  if (normalize) {
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

# formating info message
info = function(...) {
  if (getOption("batchtools.verbose", TRUE))
    message(sprintf(...))
}

# formating debug message
debug = function(...) {
  if (getOption("batchtools.debug", FALSE))
    message(sprintf(...))
}

# formating cat()
catf = function (..., con = "") {
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

findConfFile = function() {
  uris = file.path(c(".", "~"), c("batchtools.conf.R", ".batchtools.conf.R"))
  i = wf(file.exists(uris))
  if (length(i) == 0L) character(0L) else npath(uris[i])
}

findTemplateFile = function(name) {
  uris = c(
    sprintf("batchtools.%s.tmpl", name),
    file.path("~", sprintf(".batchtools.%s.tmpl", name)),
    system.file("templates", sprintf("%s.default.tmpl", name), package = "batchtools")
  )
  i = wf(nzchar(uris) & file.exists(uris))
  if (length(i) == 0L) character(0L) else npath(uris[i])
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
