context("batchReduce")

test_that("batchReduce", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  ids = batchReduce(function(aggr, x) aggr+x, 1:20, init=0, blocks = chunk(seq_along(xs), n.chunks = 10), reg = reg)
  submitJobs(reg)
  waitForJobs(reg)
  y = reduceResults(reg, fun=function(aggr, job, res) aggr+res, init=0)
  expect_equal(y, sum(1:20))
  expect_equal(ids, 1:2)

  reg = makeTestRegistry()
  ids = batchReduce(reg, function(aggr, x, y) aggr+x+y, 1:20, init=0, block.size=10,
    more.args=list(y=1))
  submitJobs(reg)
  waitForJobs(reg)
  y = reduceResults(reg, fun=function(aggr, job, res) aggr+res, init=0)
  expect_equal(y, sum(1:20)+20)

  reg = makeTestRegistry()
  expect_equal(batchReduce(reg, function(aggr, x) aggr+x, integer(0L), init=0, block.size=10), integer(0L))
})
