context("measure memory")

test_that("memory measurements work", {
  skip_on_os("windows")
  skip_on_travis()
  skip_on_cran()

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsSSH(list(Worker$new("localhost")))
  ids = batchMap(function(n) { m = matrix(runif(n), nrow = 10); m %*% t(m) }, n = c(100, 1e7), reg = reg)
  submitAndWait(reg, 1:2, resources = list(measure.memory = TRUE))

  expect_true(any(stri_detect_fixed(readLog(1L, reg = reg)$lines, "Memory measurement enabled")))
  expect_numeric(reg$status$mem.used, any.missing = FALSE)
  expect_true(reg$status$mem.used[2] > reg$status$mem.used[1])
})
