context("waitForJobs")

test_that("waitForJobs", {
  reg = makeTempRegistry(make.default = FALSE)
  fun = function(x) if (x == 2) stop(x) else x
  ids = batchMap(reg = reg, fun, 1:2)
  submitJobs(ids, reg = reg)
  expect_true(waitForJobs(ids = ids[1], reg = reg))
  expect_false(waitForJobs(ids = ids, stop.on.error = TRUE, reg = reg))
})

test_that("waitForJobs: detection of expired jobs", {
  skip_on_cran()

  reg = makeTempRegistry(make.default = FALSE)
  if (is.null(reg$cluster.functions$killJobs))
    skip("Test requires killJobs")

  ids = batchMap(reg = reg, Sys.sleep, c(20, 20))
  submitJobs(ids, reg = reg)
  batch.ids = reg$status$batch.id
  reg$cluster.functions$killJob(reg, batch.ids[1])
  expect_false(expect_warning(waitForJobs(ids, reg = reg)))
})
