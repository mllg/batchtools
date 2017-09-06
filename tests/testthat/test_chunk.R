context("chunk")

test_that("chunk", {
  x = 1:10; n.chunks = 2
  expect_integer(chunk(x, n.chunks = n.chunks), len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  x = 1:10; n.chunks = 1
  expect_integer(chunk(x, n.chunks = n.chunks), len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  x = 1:10; n.chunks = 10
  expect_integer(chunk(x, n.chunks = n.chunks), len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  x = 1:10; n.chunks = 20
  expect_integer(chunk(x, n.chunks = n.chunks), len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  x = integer(0); n.chunks = 20
  expect_integer(chunk(x, n.chunks = n.chunks), len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)

  x = 1:10; chunk.size = 3
  res = chunk(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_integer(table(res), lower = 1, upper = chunk.size, any.missing = FALSE)

  x = 1:10; chunk.size = 1
  res = chunk(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_integer(table(res), lower = 1, upper = chunk.size, any.missing = FALSE)

  expect_equal(chunk(numeric(0), chunk.size = 1), integer(0))
  expect_equal(chunk(numeric(0), n.chunks = 1), integer(0))
})

test_that("binpack", {
  x = 1:10; chunk.size = 10
  res = binpack(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), lower = min(x), upper = chunk.size, any.missing = FALSE)

  x = 1; chunk.size = 10
  res = binpack(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), lower = min(x), upper = chunk.size, any.missing = FALSE)

  x = rep(1, 100); chunk.size = 1
  res = binpack(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), lower = min(x), upper = chunk.size, any.missing = FALSE)

  x = runif(100); chunk.size = 1
  res = binpack(x, chunk.size = chunk.size)
  expect_integer(res, len = length(x), lower = 1, upper = length(x), any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), lower = min(x), upper = chunk.size, any.missing = FALSE)

  expect_equal(binpack(numeric(0), 1), integer(0))
})

test_that("lpt", {
  x = 1:10; n.chunks = 2
  res = lpt(x, n.chunks)
  expect_integer(res, len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), len = min(length(x), n.chunks), lower = min(x), any.missing = FALSE)

  x = runif(100); n.chunks = 3
  res = lpt(x, n.chunks)
  expect_integer(res, len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), len = min(length(x), n.chunks), lower = min(x), any.missing = FALSE)

  x = 1:10; n.chunks = 1
  res = lpt(x, n.chunks)
  expect_integer(res, len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), len = min(length(x), n.chunks), lower = min(x), any.missing = FALSE)

  x = 1:10; n.chunks = 12
  res = lpt(x, n.chunks)
  expect_integer(res, len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), len = min(length(x), n.chunks), lower = min(x), any.missing = FALSE)
  expect_equal(unname(res), 10:1)

  expect_equal(lpt(numeric(0), 1), integer(0))
})

test_that("caching works", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  p1 = addProblem(reg = reg, "p1", data = iris)
  p2 = addProblem(reg = reg, "p2", data = data.frame(a = 1:10))
  a = addAlgorithm(reg = reg, name = "a", fun = function(data, ...) nrow(data))

  addExperiments(reg = reg)
  ids = findJobs(reg = reg)
  ids$chunk = 1L
  submitJobs(ids, reg = reg)

  expect_equal(unlist(reduceResultsList(ids, reg = reg)), c(150, 10))
})
