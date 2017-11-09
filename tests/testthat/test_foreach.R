context("foreach")

silent({
  reg = makeTestRegistry(packages = "foreach")
  fun = function(i) {
    foreach(j = 1:2) %dopar% { Sys.sleep(3); i }
  }
  ids = batchMap(fun, i = 1, reg = reg)
})

test_that("pm/multicore", {
  skip_if_not_installed("foreach")
  skip_if_not_installed("doParallel")
  if (reg$cluster.functions$name %chin% c("Parallel", "Socket"))
    skip("Nested local parallelization not supported")

  submitAndWait(reg, ids = ids, resources = list(foreach.backend = "parallel", ncpus = 2))
  expect_equal(nrow(findDone(reg = reg)), 1L)
  status = getJobStatus(reg = reg)
  expect_true(status$time.running < 5.9)
})
