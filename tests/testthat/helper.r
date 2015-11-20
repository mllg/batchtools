library("data.table")
library("checkmate")
library("stringi")
requireNamespace("withr")

silent = function(expr) {
  withr::with_options(list(batchtools.progress = FALSE), force(expr))
}

expect_data_table = function(tab, key = NULL, ...) {
  expect_is(tab, "data.table")
  expect_data_frame(tab, ...)
  if (!is.null(key))
    expect_identical(key(tab), key)
}

checkTables = function(reg, ...) {
  cols = c("def.id", "pars", "pars.hash")
  types = c("integer", "list", "character")
  expect_is(reg$defs, "data.table")
  expect_data_table(reg$defs, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$defs), cols)
  expect_equal(as.character(reg$defs[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$defs), "def.id")
  expect_equal(uniqueN(reg$defs$def.id), nrow(reg$defs))

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
  expect_equal(uniqueN(reg$status$job.id), nrow(reg$status))

  cols = c("resource.id", "resources.hash", "resources")
  types = c("integer", "character", "list")
  expect_is(reg$resources, "data.table")
  expect_data_table(reg$resources, ncols = length(cols), ...)
  expect_set_equal(colnames(reg$resources), cols)
  expect_equal(as.character(reg$resources[, lapply(.SD, class), .SDcols = cols]), types)
  expect_equal(key(reg$resources), "resource.id")
  expect_equal(uniqueN(reg$resources$resource.id), nrow(reg$resources))

  expect_set_equal(reg$defs$def.id, reg$status$def.id)
}
