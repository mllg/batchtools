context("measure memory")

test_that("memory measurements work", {
  reg = makeTempRegistry(make.default = FALSE)
  reg$default.resources$measure.memory = TRUE
  ids = batchMap(function(n) { m = matrix(runif(n), nrow = 10); m %*% m }, n = c(100, 1e7), reg = reg)
  submitJobs(chunkIds(1:2, reg = reg), reg = reg)
  expect_true(any(stri_detect_fixed(readLog(1L, reg = reg), "Enabling memory measurement")))
  syncRegistry(reg = reg)
  expect_numeric(reg$status$memory, any.missing = FALSE)
  expect_true(reg$status$memory[2] > reg$status$memory[1])
})
