context("Results")

suppressMessages({
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, a = 1:4, b = 4:1, reg = reg)
  submitAndWait(reg, 1:3)
})

test_that("loadResult", {
  expect_equal(loadResult(reg = reg, 1), list(a = 1, b = 4))
  expect_equal(loadResult(reg = reg, 2), list(a = 2, b = 3))
  expect_error(loadResult(reg = reg, 4), "not terminated")
})

test_that("reduceResults", {
  silent({
    expect_equal(reduceResults(fun = function(aggr, res, ...) c(aggr, res$a), init = integer(0), reg = reg), 1:3)
    expect_equal(reduceResults(ids = 1, fun = c, reg = reg), list(a = 1, b = 4))
    expect_equal(reduceResults(ids = 1, fun = c, list(c = 1), reg = reg)$c, 1)
    expect_equal(reduceResults(fun = function(aggr, res, extra.arg, ...) aggr + res$a + extra.arg, init = 0, extra.arg = 1, reg = reg), sum(1:3 + 1))
    expect_equal(reduceResults(fun = function(job, aggr, res) c(aggr, job$id), init = integer(0), ids = 2:3, reg = reg), 2:3)
    expect_list(reduceResults(fun = function(job, aggr, res) c(aggr, list(job)), init = list(), ids = 2:3, reg = reg), types = "Job", len = 2)

    expect_equal(
      reduceResults(fun = function(aggr, res, ...) c(aggr, res$a), ids = 3:1, init = integer(0), reg = reg),
      rev(reduceResults(fun = function(aggr, res, ...) c(aggr, res$a), ids = 1:3, init = integer(0), reg = reg))
    )
    expect_error(reduceResults(fun = function(aggr, res, ...) c(aggr, res$a), ids = 1:4, init = integer(0), reg = reg),
      "successfully computed")
  })
})

test_that("reduceResultsList", {
  silent({
    expect_equal(reduceResultsList(reg = reg), Map(fun, a = 1:3, b = 4:2))
    expect_equal(reduceResultsList(reg = reg, fun = function(x) x$a), as.list(1:3))
    expect_equal(reduceResultsList(reg = reg, fun = function(x, y) x$a + y, y = 1), as.list(1:3 + 1))
    expect_list(reduceResultsList(reg = reg, fun = function(job, ...) job), types = "Job", len = 3)
    expect_equal(reduceResultsList(ids = 2:1, reg = reg), rev(reduceResultsList(ids = 1:2, reg = reg)))
  })
})



test_that("reduceResultsDataTable", {
  silent({
    tab = reduceResultsDataTable(reg = reg)
    expect_data_table(tab, nrow = 3, ncol = 3, key = "job.id")
    expect_null(tab$result)
    expect_equal(tab$a, 1:3)

    tab = reduceResultsDataTable(reg = reg, fun = function(x) list(a = x$a))
    expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
    expect_equal(tab$a, 1:3)

    tab = reduceResultsDataTable(reg = reg, ids = 3:2, fun = function(x) list(a = x$a))
    expect_data_table(tab, nrow = 2, ncol = 2, key = "job.id")
    expect_equal(tab$a, 2:3)

    tab = reduceResultsDataTable(reg = reg, fun = function(x) x$a)
    expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
    expect_equal(tab$V1, 1:3)

    tab = reduceResultsDataTable(reg = reg, fun = function(x, y) x$a + y, y = 1)
    expect_data_table(tab, nrow = 3, ncol = 2, key = "job.id")
    expect_equal(tab$V1, 1:3 + 1L)
  })
})

test_that("reduceResults with no results reg", {
  silent({
    reg = makeRegistry(file.dir = NA, make.default = FALSE)

    expect_equal(reduceResults(fun = c, reg = reg), NULL)
    expect_equal(reduceResults(fun = c, reg = reg, init = 42), 42)
    expect_equal(reduceResultsList(reg = reg), list())

    fun = function(...) list(...)
    ids = batchMap(fun, a = 1:3, b = 3:1, reg = reg)

    expect_equal(reduceResults(fun = c, reg = reg), NULL)
    expect_equal(reduceResults(fun = c, reg = reg, init = 42), 42)
    expect_equal(reduceResultsList(reg = reg), list())
  })
})

test_that("reduceResultsList/NULL", {
  reg = makeRegistry(NA, make.default = FALSE)
  f = function(...) NULL
  ids = batchMap(f, 1:3, reg = reg)
  submitAndWait(ids, reg = reg)
  res = reduceResultsList(ids = ids, reg = reg)
  expect_equal(res, replicate(3, NULL, simplify = FALSE))
})

test_that("batchMapResults", {
  target = makeRegistry(NA, make.default = FALSE)
  x = batchMapResults(target = target, function(x, c, d) x$a+x$b + c + d, c = 11:13, source = reg, more.args = list(d = 2))
  expect_data_table(x, nrow = 3, key = "job.id")
  expect_data_table(target$status, nrow = 3)
  submitAndWait(target)
  res = reduceResultsDataTable(reg = target)
  expect_equal(res[[2L]], 11:13 + rep(5, 3) + 2)
})

test_that("multiRowResults", {
  silent({
    reg = makeRegistry(file.dir = NA, make.default = FALSE)
    fun = function(a) data.table(y1 = rep(a, 3), y2 = rep(a/2, 3))
    ids = batchMap(fun, a = c(10, 100), reg = reg)
    submitAndWait(reg, ids)
    expect_error(reduceResultsDataTable(reg = reg), "one row")
  })
})

suppressMessages({
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", fun = function(job, data, ...) 2, seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  ids = addExperiments(list(p1 = data.table()), list(a1 = data.table(sq = 1:3)), reg = reg)
  submitAndWait(reg = reg)
})

test_that("reduceResults/BatchExperiments", {
  silent({
    expect_equal(reduceResults(fun = function(aggr, res, ...) c(aggr, res), init = integer(0), reg = reg), 2^(1:3))
    expect_equal(reduceResults(ids = 2:3, fun = function(aggr, job, res, ...) c(aggr, job$id), init = integer(0), reg = reg), 2:3)
    expect_list(reduceResults(fun = function(job, aggr, res) c(aggr, list(job)), init = list(), ids = 2:3, reg = reg), types = "Experiment", len = 2)
  })
})

test_that("reduceResultsList/BatchExperiments", {
  silent({
    expect_equal(reduceResultsList(reg = reg), as.list(2^(1:3)))
    expect_equal(reduceResultsList(fun = function(job, ...) job$prob.name, reg = reg), as.list(rep("p1", 3)))
    expect_equal(reduceResultsList(fun = function(job, ...) job$algo.name, reg = reg), as.list(rep("a1", 3)))
    expect_equal(reduceResultsList(fun = function(job, ...) job$instance, reg = reg), as.list(rep(2, 3)))
  })
})

