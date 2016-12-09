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
})

test_that("lpt", {
  x = 1:10; n.chunks = 2
  res = lpt(x, n.chunks)
  expect_integer(res, len = length(x), lower = 1, upper = n.chunks, any.missing = FALSE)
  expect_numeric(sapply(split(x, res), sum), len = min(length(x), n.chunks), lower = min(x), any.missing = FALSE)

  x = runif(100); n.chunks = 2
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
})
