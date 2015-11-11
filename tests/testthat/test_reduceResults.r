context("Results")

suppressMessages({
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, a = 1:4, b = 4:1, reg = reg)
  submitJobs(reg = reg, ids = chunkIds(1:3, n.chunks = 1, reg = reg))
  waitForJobs(reg = reg)
})

test_that("reduceResults with no results reg", {
  reg = makeTempRegistry(FALSE)

  expect_equal(reduceResults(fun = c, reg = reg), NULL)
  expect_equal(reduceResults(fun = c, reg = reg, init = 42), 42)
  expect_equal(reduceResultsList(reg = reg), list())

  fun = function(...) list(...)
  ids = batchMap(fun, a = 1:3, b = 3:1, reg = reg)

  expect_equal(reduceResults(fun = c, reg = reg), NULL)
  expect_equal(reduceResults(fun = c, reg = reg, init = 42), 42)
  expect_equal(reduceResultsList(reg = reg), list())
})

test_that("reduceResults", {
  expect_equal(reduceResults(fun = function(aggr, res, ...) c(aggr, res$a), init = integer(0), reg = reg), 1:3)
  expect_equal(reduceResults(ids = 1, fun = c, reg = reg), list(a = 1, b = 4))
  expect_equal(reduceResults(ids = 1, fun = c, list(c = 1), reg = reg)$c, 1)
  expect_equal(reduceResults(fun = function(aggr, res, extra.arg, ...) aggr + res$a + extra.arg, init = 0, extra.arg = 1, reg = reg), sum(1:3 + 1))
  expect_equal(reduceResults(fun = function(job, aggr, res) c(aggr, job$defs$job.id), init = integer(0), ids = 2:3, reg = reg), 2:3)
})

test_that("reduceResultsList", {
  expect_equal(reduceResultsList(reg = reg), Map(fun, a = 1:3, b = 4:2))
  expect_equal(reduceResultsList(reg = reg, fun = function(x) x$a), as.list(1:3))
  expect_equal(reduceResultsList(reg = reg, fun = function(x, y) x$a + y, y = 1), as.list(1:3 + 1))
})

test_that("reduceResultsDataTable", {
  tab = reduceResultsDataTable(reg = reg)
  expect_data_table(tab, nrow = 3, ncol = 3, key = "job.id")
  expect_null(tab$result)
  expect_equal(tab$a, 1:3)

  tab = reduceResultsDataTable(reg = reg, fun = function(x) list(a = x$a))
  expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
  expect_equal(tab$a, 1:3)

  tab = reduceResultsDataTable(reg = reg, fun = function(x) x$a)
  expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
  expect_equal(tab$V1, 1:3)

  tab = reduceResultsDataTable(reg = reg, fun = function(x, y) x$a + y, y = 1)
  expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
  expect_equal(tab$V1, 1:3 + 1L)
})

test_that("loadResult", {
  expect_equal(loadResult(reg = reg, 1), list(a = 1, b = 4))
  expect_equal(loadResult(reg = reg, 2), list(a = 2, b = 3))
  expect_null(loadResult(reg = reg, 4))
  expect_identical(loadResult(reg = reg, 4, missing.val = NA_real_), NA_real_)
})

test_that("multiRowResults", {
  reg = makeTempRegistry(FALSE)
  fun = function(a) data.table(y1 = rep(a, 3), y2 = rep(a/2, 3))
  ids = batchMap(fun, a = c(10, 100), reg = reg)
  submitJobs(reg = reg)
  waitForJobs(reg = reg)
  expect_error(reduceResultsDataTable(reg = reg), "one row")
})
