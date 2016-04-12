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
  with_options(list(batchtools.progress = FALSE), expr)
}

expect_data_table = function(tab, key = NULL, ...) {
  expect_is(tab, "data.table")
  expect_data_frame(tab, ...)
  if (!is.null(key))
    expect_identical(key(tab), key)
}

checkTables = function(reg, ...) {
  if (class(reg)[1L] == "Registry") {
    cols = c("def.id", "pars", "pars.hash")
    types = c("integer", "list", "character")
  } else {
    cols = c("def.id", "pars", "problem", "algorithm", "pars.hash")
    types = c("integer", "list", "factor", "factor", "character")
  }
  expect_is(reg$defs, "data.table")
  expect_data_table(reg$defs, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$defs), cols)
  expect_equal(as.character(reg$defs[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$defs), "def.id")
  expect_equal(uniqueN(reg$defs$def.id), nrow(reg$defs))

  if (class(reg)[1L] == "Registry") {
    cols = c("job.id", "def.id", "submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash")
    types = c("integer", "integer", "integer", "integer", "integer", "character", "numeric", "factor", "character", "character")
  } else {
    cols = c("job.id", "def.id", "submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash", "repl")
    types = c("integer", "integer", "integer", "integer", "integer", "character", "numeric", "factor", "character", "character", "integer")
  }
  expect_is(reg$status, "data.table")
  expect_data_table(reg$status, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$status), cols)
  expect_equal(as.character(reg$status[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$status), "job.id")
  expect_equal(uniqueN(reg$status$job.id), nrow(reg$status))

  cols = c("resource.id", "resources")
  types = c("factor", "list")
  expect_is(reg$resources, "data.table")
  expect_data_table(reg$resources, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$resources), cols)
  expect_equal(as.character(reg$resources[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$resources), "resource.id")
  expect_equal(uniqueN(reg$resources$resource.id), nrow(reg$resources))

  if (class(reg)[1L] == "ExperimentRegistry") {
    expect_integer(reg$status$repl, lower = 1L, any.missing = FALSE)
  }

  expect_set_equal(reg$defs$def.id, reg$status$def.id)
}
