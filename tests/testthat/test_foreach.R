test_that("foreach/seq", {
  skip_if_not_installed("foreach")
  reg = makeTestRegistry(packages = "foreach")
  fun = function(i) {
    foreach(j = 1:i, .combine = c) %dopar% { j^2 }
  }
  ids = batchMap(fun, i = 1:2, reg = reg)
  submitAndWait(reg, ids = ids, resources = list(foreach.backend = "seq", ncpus = 2))
  expect_equal(nrow(findDone(reg = reg)), 2L)
  expect_equal(reduceResultsList(reg = reg), list(1, c(1, 4)))
})

test_that("foreach/multicore", {
  skip_if_not_installed("foreach")
  skip_if_not_installed("doParallel")
  reg = makeTestRegistry(packages = "foreach")
  if (reg$cluster.functions$name %chin% c("Parallel", "Socket"))
    skip("Nested local parallelization not supported")

  fun = function(i) {
    foreach(j = 1:2) %dopar% { Sys.sleep(3); i }
  }
  ids = batchMap(fun, i = 1, reg = reg)

  submitAndWait(reg, ids = ids, resources = list(foreach.backend = "parallel", ncpus = 2))
  expect_equal(nrow(findDone(reg = reg)), 1L)
  status = getJobStatus(reg = reg)
  expect_true(status$time.running < 5.9)
  expect_equal(reduceResultsList(reg = reg), list(as.list(c(1, 1))))
})
