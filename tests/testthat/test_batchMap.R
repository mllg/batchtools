context("batchMap")

test_that("batchMap", {
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, more.args = list(x = 1), reg = reg)
  expect_data_table(ids, any.missing = FALSE, ncols = 1L, nrow = 3L, key = "job.id")
  expect_equal(ids$job.id, 1:3)
  expect_equal(readRDS(file.path(reg$file.dir, "user.function.rds")), fun)
  expect_equal(readRDS(file.path(reg$file.dir, "more.args.rds")), list(x = 1))

  checkTables(reg)
  expect_data_table(reg$defs, nrow = 3L, any.missing = FALSE)
  expect_data_table(reg$status, nrow = 3L)
  expect_data_table(reg$resources, nrow = 0L)
  expect_equal(reg$defs$pars[[1L]], list(i = 1))

  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, 1:3, more.args = list(j = 1), reg = reg)
  expect_equal(readRDS(file.path(reg$file.dir, "more.args.rds")), list(j = 1))
  expect_equal(reg$defs$pars, lapply(1:3, list))

  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, j = 1L, reg = reg)
  expect_identical(reg$defs$pars[[3L]], list(i = 3L, j = 1L))
})
