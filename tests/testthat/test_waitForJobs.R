context("waitForJobs")

test_that("waitForJobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(x) if (x == 2) stop(x) else x
  ids = batchMap(reg = reg, fun, 1:2)
  silent({
    submitJobs(ids, reg = reg)
    expect_true(waitForJobs(ids = ids[1], reg = reg))
    expect_false(waitForJobs(ids = ids, stop.on.error = TRUE, reg = reg))
  })
})

test_that("waitForJobs: detection of expired jobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  if (is.null(reg$cluster.functions$killJob))
    skip("Test requires killJobs")

  ids = batchMap(reg = reg, Sys.sleep, c(20, 20))
  silent({
    submitJobs(ids, reg = reg)
    batch.ids = reg$status$batch.id
    reg$cluster.functions$killJob(reg, batch.ids[1])
    expect_warning(waitForJobs(ids, reg = reg, sleep = 1))
  })
})

test_that("waitForJobs: filter out unsubmitted jobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  ids = batchMap(identity, 1:2, reg = reg)
  silent({
    submitJobs(ids = 1, reg = reg)
    expect_warning(res <- waitForJobs(ids = ids, reg = reg), "unsubmitted")
    expect_true(res)
  })
})
