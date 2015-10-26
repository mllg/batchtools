context("submitJobs")

test_that("submitJobs", {
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg)

  submitJobs(chunkIds(1:2, reg = reg), reg = reg)
  waitForJobs(reg = reg)
  checkTables(reg)

  expect_integer(reg$status[1:2, resource.id], any.missing = FALSE)
  expect_character(reg$status[1:2, batch.id], any.missing = FALSE)
  expect_integer(reg$status[1:2, submitted], any.missing = FALSE)
  expect_true(is.na(reg$status[3, submitted]))
  x = reg$resources[1, resources][[1L]]
  y = reg$default.resources
  expect_equal(x[order(names2(x))], y[order(names2(y))])

  submitJobs(3, resources = list(walltime = 100, memory = 500), reg = reg)
  waitForJobs(reg = reg)
  res = reg$resources[2, resources][[1L]]
  expect_equal(res$walltime, 100)
  expect_equal(res$memory, 500)

  # should be 2 chunks?
  expect_equal(uniqueN(reg$status$job.hash), 2)
})
