test_that("waitForJobs", {
  reg = makeTestRegistry()
  fun = function(x) if (x == 2) stop(x) else x
  ids = batchMap(reg = reg, fun, 1:2)
  silent({
    submitJobs(ids, reg = reg)
    expect_true(waitForJobs(ids = ids[1], reg = reg, sleep = 1))
    expect_false(waitForJobs(ids = ids, stop.on.error = TRUE, sleep = 1, expire.after = 3, reg = reg))
  })
})

test_that("waitForJobs: detection of expired jobs", {
  reg = makeTestRegistry()
  if (is.null(reg$cluster.functions$killJob))
    skip("Test requires killJobs")
  ids = batchMap(reg = reg, Sys.sleep, c(20, 20))
  ids$chunk = 1L

  silent({
    submitJobs(ids, reg = reg)
    batch.ids = reg$status$batch.id
    reg$cluster.functions$killJob(reg, batch.ids[1])
    expect_warning(waitForJobs(ids, reg = reg, sleep = 1, stop.on.expire = TRUE), "disappeared")
  })
})

test_that("waitForJobs: filter out unsubmitted jobs", {
  reg = makeTestRegistry()
  ids = batchMap(identity, 1:2, reg = reg)
  silent({
    submitJobs(ids = 1, reg = reg)
    expect_warning(res <- waitForJobs(ids = ids, reg = reg, sleep = 1), "unsubmitted")
    expect_true(res)
  })
})
