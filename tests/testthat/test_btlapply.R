context("btlapply")

test_that("btlapply", {
  reg = makeRegistry(NA, make.default = FALSE)
  fun = function(x, y) x^y
  res = silent(btlapply(1:3, fun, y = 2, n.chunks = 2, resources = list(..dummy = 42), reg = reg))
  expect_equal(res, lapply(1:3, fun, y = 2))
  expect_equal(uniqueN(reg$status$job.hash), 2)
  expect_equal(reg$resources$resources[[1L]]$..dummy, 42)
})

test_that("btmapply", {
  fun = function(x, y) paste0(x, y)
  x = 1:3
  y = letters[1:3]

  reg = makeRegistry(NA, make.default = FALSE)
  res = silent(btmapply(fun, x = x, y = y, chunk.size = 2, use.names = FALSE, reg = reg))
  expect_equal(res, mapply(fun, x = x, y = y, SIMPLIFY = FALSE, USE.NAMES = FALSE))
  expect_equal(uniqueN(reg$status$job.hash), 2)

  reg = makeRegistry(NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsInteractive()
  expect_equal(silent(btmapply(fun, x = x, y = y, n.chunks = 1, use.names = FALSE, simplify = TRUE, reg = reg)), mapply(fun, x = x, y = y, SIMPLIFY = TRUE, USE.NAMES = FALSE))

  reg = makeRegistry(NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsInteractive()
  expect_equal(silent(btmapply(fun, x = x, y = y, n.chunks = 1, use.names = TRUE, simplify = TRUE, reg = reg)), mapply(fun, x = x, y = y, SIMPLIFY = TRUE, USE.NAMES = TRUE))
})
