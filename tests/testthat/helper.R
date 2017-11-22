library("testthat")
library("data.table")
library("checkmate")
library("stringi")
requireNamespace("withr")

makeTestRegistry = function(file.dir = NA, make.default = FALSE, ...) {
  reg = makeRegistry(file.dir = file.dir, make.default = make.default, ...)
  fd = reg$file.dir
  reg.finalizer(e = reg, f = function(reg) unlink(fd, recursive = TRUE), onexit = TRUE)
  return(reg)
}

makeTestExperimentRegistry = function(file.dir = NA, make.default = FALSE, ...) {
  reg = makeExperimentRegistry(file.dir = file.dir, make.default = make.default, ...)
  fd = reg$file.dir
  reg.finalizer(e = reg, f = function(reg) unlink(fd, recursive = TRUE), onexit = TRUE)
  return(reg)
}

silent = function(expr) {
  withr::with_options(list(batchtools.progress = FALSE, batchtools.verbose = FALSE), expr)
}

s.chunk = function(ids) {
  ids$chunk = 1L
  ids
}

submitAndWait = function(reg, ids = NULL, ..., sleep = 1) {
  ids = if (is.null(ids)) findNotSubmitted(reg = reg) else convertIds(reg, ids, keep.extra = names(ids))
  if ("chunk" %chnin% names(ids))
    ids = s.chunk(ids)
  silent({
    ids = submitJobs(ids = ids, ..., reg = reg)
    waitForJobs(ids, reg = reg, sleep = sleep)
  })
}

suppressAll = function (expr) {
  silent(capture.output({z = suppressWarnings(suppressMessages(suppressPackageStartupMessages(force(expr))))}))
  invisible(z)
}

checkTables = function(reg, ...) {
  expect_is(reg$mtime, "POSIXct")

  if (class(reg)[1L] == "Registry") {
    cols = c("def.id",   "job.pars")
    types = c("integer", "list")
  } else {
    cols = c("def.id",   "problem",   "prob.pars", "algorithm", "algo.pars", "pars.hash")
    types = c("integer", "character", "list",      "character", "list",      "character")
  }
  expect_is(reg$defs, "data.table")
  expect_data_table(reg$defs, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$defs), cols)
  expect_equal(as.character(reg$defs[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$defs), "def.id")
  expect_equal(anyDuplicated(reg$defs, by = "def.id"), 0L)

  if (class(reg)[1L] == "Registry") {
    cols  = c("job.id",  "def.id",  "submitted", "started", "done",    "error",     "memory",  "resource.id", "batch.id",  "log.file", "job.hash", "job.name")
    types = c("integer", "integer", "numeric",   "numeric", "numeric", "character", "numeric", "integer",     "character", "character",  "character", "character")
  } else {
    cols  = c("job.id",  "def.id",  "submitted", "started", "done",    "error",     "memory",  "resource.id", "batch.id",  "log.file", "job.hash",  "job.name", "repl")
    types = c("integer", "integer", "numeric",   "numeric", "numeric", "character", "numeric", "integer",     "character", "character",  "character", "character", "integer")
  }
  expect_is(reg$status, "data.table")
  expect_data_table(reg$status, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$status), cols)
  expect_equal(as.character(reg$status[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$status), "job.id")
  expect_equal(anyDuplicated(reg$status, by = "job.id"), 0L)
  checkStatusIntegrity(reg)

  cols = c("resource.id", "resource.hash", "resources")
  types = c("integer", "character", "list")
  expect_data_table(reg$resources, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$resources), cols)
  expect_equal(as.character(reg$resources[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$resources), "resource.id")
  expect_equal(anyDuplicated(reg$resources, by = "resource.id"), 0L)

  cols = c("job.id", "tag")
  types = c("integer", "character")
  expect_data_table(reg$tags, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$tags), cols)
  expect_equal(as.character(reg$tags[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$tags), "job.id")

  if (class(reg)[1L] == "ExperimentRegistry") {
    expect_integer(reg$status$repl, lower = 1L, any.missing = FALSE)
    expect_subset(reg$defs$problem, reg$problems)
    expect_subset(reg$defs$algorithm, reg$algorithms)
  }

  expect_key_set_equal(reg$defs, reg$status, by = "def.id")
  expect_key_set_equal(reg$status[!is.na(resource.id)], reg$resources, by = "resource.id")
  if (nrow(reg$status) > 0L)
    expect_data_table(ajoin(reg$tags, reg$status, by = "job.id"), nrow = 0)
  else
    expect_equal(nrow(reg$tags), 0)
}

checkStatusIntegrity = function(reg) {
  tab = reg$status[, list(job.id, code = (!is.na(submitted)) + 2L * (!is.na(started)) + 4L * (!is.na(done)) + 8L * (!is.na(error)))]

  # submitted   started   done   error
  #       2^0       2^1    2^2     2^3
  #         1         2      4       8
  # ------------------------------------------------------
  #         0         0      0       0 -> 0  (unsubmitted)
  #         1         0      0       0 -> 1  (submitted)
  #         1         1      0       0 -> 3  (started)
  #         1         1      1       0 -> 7  (done)
  #         1         1      1       1 -> 15 (error)

  expect_subset(tab$code, c(0L, 1L, 3L, 7L, 15L), info = "Status Integrity")
}

expect_copied = function(x, y) {
  expect_false(data.table:::address(x) == data.table:::address(y))
}

expect_key_set_equal = function(x, y, by = NULL) {
  expect_true(nrow(ajoin(x, y, by = by)) == 0 && nrow(ajoin(y, x, by = by)) == 0)
}
