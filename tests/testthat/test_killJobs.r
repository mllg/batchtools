context("killJobs")

test_that("killJobs", {
  reg = makeTempRegistry(make.default = FALSE)
  if (is.null(reg$cluster.functions$killJobs))
    skip("Test requires killJobs")

  reg = makeTempRegistry(FALSE)
  ids = batchMap(Sys.sleep, time = 60, reg = reg)
  submitJobs(1, reg = reg)
  expect_equal(findOnSystem(1, reg = reg), findJobs(reg = reg))

  batch.id = reg$status[1, batch.id]
  res = killJobs(1, reg = reg)
  expect_equal(res$job.id, 1L)
  expect_equal(res$batch.id, batch.id)
  expect_true(res$killed)
})
