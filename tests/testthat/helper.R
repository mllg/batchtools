library("data.table")
library("checkmate")
library("stringi")

with_options = function(opts, expr) {
  prev = options(names(opts))
  on.exit(do.call(options, prev))
  do.call(options, opts)
  force(expr)
}

silent = function(expr) {
  with_options(list(batchtools.progress = FALSE, batchtools.verbose = FALSE), expr)
}

submitAndWait = function(reg, ids = NULL, ...) {
  if (is.null(ids))
    ids = findNotSubmitted(reg = reg)
  if ("chunk" %nin% names(ids))
    ids = chunkIds(ids, n.chunks = 1L, reg = reg)
  silent({
    ids = submitJobs(ids = ids, ..., reg = reg)
    waitForJobs(ids, reg = reg)
  })
}

suppressAll = function (expr) {
  silent(capture.output({z = suppressWarnings(suppressMessages(suppressPackageStartupMessages(force(expr))))}))
  invisible(z)
}

checkTables = function(reg, ...) {
  if (class(reg)[1L] == "Registry") {
    cols = c("def.id", "pars")
    types = c("integer", "list")
  } else {
    cols = c("def.id", "pars", "problem", "algorithm", "pars.hash")
    types = c("integer", "list", "factor", "factor", "character")
  }
  expect_is(reg$defs, "data.table")
  expect_data_table(reg$defs, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$defs), cols)
  expect_equal(as.character(reg$defs[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$defs), "def.id")
  expect_equal(anyDuplicated(reg$defs, by = "def.id"), 0L)

  if (class(reg)[1L] == "Registry") {
    cols = c("job.id", "def.id", "submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash")
    types = c("integer", "integer", "integer", "integer", "integer", "character", "numeric", "integer", "character", "character")
  } else {
    cols = c("job.id", "def.id", "submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash", "repl")
    types = c("integer", "integer", "integer", "integer", "integer", "character", "numeric", "integer", "character", "character", "integer")
  }
  expect_is(reg$status, "data.table")
  expect_data_table(reg$status, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$status), cols)
  expect_equal(as.character(reg$status[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$status), "job.id")
  expect_equal(anyDuplicated(reg$status, by = "job.id"), 0L)

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
  }

  expect_set_equal(reg$defs$def.id, reg$status$def.id)
  expect_set_equal(na.omit(reg$status$resource.id), reg$resources$resource.id)
  if (nrow(reg$status) > 0L)
    expect_subset(reg$tags$job.id, reg$status$job.id)
  else
    expect_equal(nrow(reg$tags), 0)
}

expect_copied = function(x, y) {
  expect_false(data.table:::address(x) == data.table:::address(y))
}
