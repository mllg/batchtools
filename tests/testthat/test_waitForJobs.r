context("waitForJobs")

test_that("waitForJobs", {
  reg = makeTempRegistry(make.default = FALSE)
  fun = function(x) if (x == 2) stop(x) else x
  ids = batchMap(reg = reg, fun, 1:2)
  submitJobs(reg = reg, ids = ids)
  expect_true(waitForJobs(reg = reg, 1))
  # expect_false(waitForJobs(reg = reg, 1:2))
})
