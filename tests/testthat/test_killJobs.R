context("killJobs")

test_that("killJobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  if (is.null(reg$cluster.functions$killJob))
    skip("Test requires killJobs")

  ids = batchMap(Sys.sleep, time = 60, reg = reg)
  silent(submitJobs(1, reg = reg))
  expect_equal(findOnSystem(1, reg = reg), findJobs(reg = reg))

  batch.id = reg$status[1, batch.id]
  silent({
    res = killJobs(1, reg = reg)
  })
  expect_equal(res$job.id, 1L)
  expect_equal(res$batch.id, batch.id)
  expect_true(res$killed)
})
