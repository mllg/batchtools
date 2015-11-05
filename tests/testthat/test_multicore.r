context("cf multicore")

test_that("cf multicore", {
  skip_on_os("windows")

  reg = makeTempRegistry(make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(ncpus = 1L, max.load = 99, max.jobs = 99)
  ids = batchMap(Sys.sleep, time = 5, reg = reg)
  submitJobs(1, reg = reg)
  expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
  expect_true(waitForJobs(reg = reg))
  expect_equal(findDone(reg = reg), findJobs(reg = reg))
})
