test_that("submitJobs", {
  reg = makeTestRegistry()
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg)

  submitAndWait(reg, 1:2, resources = list(foo = "bar"))
  checkTables(reg)

  expect_integer(reg$status[1:2, resource.id], any.missing = FALSE)
  expect_character(reg$status[1:2, batch.id], any.missing = FALSE)
  expect_numeric(reg$status[1:2, submitted], any.missing = FALSE)
  expect_true(is.na(reg$status[3, submitted]))
  x = reg$resources[1, resources][[1L]]
  y = insert(reg$default.resources, list(foo = "bar"))
  if (isTRUE(y$chunks.as.arrayjobs) && is.na(reg$cluster.functions$array.var))
    y$chunks.as.arrayjobs = NULL
  expect_equal(x[order(names2(x))], y[order(names2(y))])

  submitAndWait(reg, 3, resources = list(walltime = 100, memory = 500))
  res = reg$resources[2, resources][[1L]]
  expect_equal(res$walltime, 100)
  expect_equal(res$memory, 500)

  # should be 2 chunks?
  expect_equal(uniqueN(reg$status$job.hash), 2)
})

test_that("per job resources", {
  reg = makeTestRegistry()

  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg)
  ids$walltime = as.integer(c(180, 120, 180))
  ids$chunk = 1:3

  submitAndWait(reg, ids = ids)

  res = reg$resources
  expect_data_table(res, nrow = 2)
  expect_equal(uniqueN(res, by = "resource.hash"), 2L)
  expect_set_equal(rbindlist(res$resources)$walltime, c(120L, 180L))

  ids$chunk = 1L
  expect_error(submitJobs(ids, reg = reg), "per-job")
})
