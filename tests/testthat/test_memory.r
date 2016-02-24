context("measure memory")

test_that("memory measurements work", {
  reg = makeTempRegistry(make.default = FALSE)
  ids = batchMap(function(n) { m = matrix(runif(n), nrow = 10); m %*% t(m) }, n = c(100, 1e7), reg = reg)
  silent({
    submitJobs(1:2, reg = reg, resources = list(measure.memory = TRUE))
    waitForJobs(reg = reg)
  })
  expect_true(any(stri_detect_fixed(readLog(1L, reg = reg), "Memory measurement enabled")))
  expect_numeric(reg$status$memory, any.missing = FALSE)
  expect_true(reg$status$memory[2] > reg$status$memory[1])
})
