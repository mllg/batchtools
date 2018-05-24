context("batchReduce")

test_that("batchReduce", {
  reg = makeTestRegistry()
  xs = 1:20
  ids = batchReduce(function(aggr, x) aggr+x, xs, init = 0, chunks = chunk(seq_along(xs), n.chunks = 10), reg = reg)
  expect_data_table(ids, nrow = 10, key = "job.id")

  submitAndWait(ids = ids, reg = reg)
  y = reduceResults(fun = function(aggr, job, res) aggr+res, init = 0, reg = reg)
  expect_equal(y, sum(1:20))
})


test_that("batchReduce w/ more.args", {
  reg = makeTestRegistry()
  xs = 1:20
  chunks = sort(chunk(seq_along(xs), n.chunks = 10))
  ids = batchReduce(function(aggr, x, y) aggr+x+y, 1:20, init = 100, chunks = chunks, more.args = list(y=1), reg = reg)
  expect_data_table(ids, nrow = 10, key = "job.id")

  submitAndWait(reg = reg)

  expect_equal(as.integer(reduceResultsList(reg = reg)), viapply(split(xs, chunks), function(x) 100L + length(x) + sum(x), use.names = FALSE))
  y = reduceResults(fun=function(aggr, job, res) aggr+res, init = 0, reg = reg)
  expect_equal(y, sum(1:20) + 20 + uniqueN(chunks) * 100)
})
