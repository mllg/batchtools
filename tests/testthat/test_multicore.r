context("cf multicore")

test_that("cf multicore", {
  skip("not now")
  skip_on_os("windows")

  reg = makeTempRegistry(make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(ncpus = 99, max.load = Inf)
  ids = batchMap(Sys.sleep, time = c(10, 5), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(killJobs(2, reg = reg)$killed)
    expect_true(waitForJobs(1, reg = reg))
    expect_equal(findDone(reg = reg), findJobs(ids = 1, reg = reg))
    expect_equal(findNotDone(reg = reg), findJobs(ids = 2, reg = reg))
  })
})

test_that("chunk parallelization works", {
  skip("Manual test")

  p = Parallel$new(4)
  reg = makeTempRegistry(TRUE, packages = "rscimark")
  batchMap(function(i) rscimark(), i = 1:8, reg = reg)
  submitJobs(chunkIds(1:8), reg = reg)
})
