context("cf multicore")

test_that("cf multicore", {
  skip_on_os("windows")

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(2)
  ids = batchMap(Sys.sleep, time = c(2, 2), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.2, reg = reg))
  })
  expect_data_table(findOnSystem(reg = reg), nrow = 0)
  expect_equal(findDone(reg = reg), findJobs(reg = reg))
})

test_that("Multicore cleans up finished processes", {
  skip("Interactive test")

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(Sys.sleep, rep(0.8, 8), reg = reg)
  parallel::mccollect()
  p = self = Multicore$new(4)

  for (i in 1:4) {
    p$spawn(makeJobCollection(i, reg = reg))
  }
  expect_data_table(p$jobs, ncol = 2)
  expect_integer(p$jobs$pid, len = 4L, any.missing = FALSE, lower = 0L)
  expect_integer(p$jobs$count, len = 4L, any.missing = FALSE, lower = 0L, upper = 1L)
  Sys.sleep(1.5)
  p$spawn(makeJobCollection(5L, reg = reg))
  expect_integer(p$jobs$pid, len = 1L, any.missing = FALSE, lower = 0L)
  p$collect(3)
  p$collect(1)
  x = parallel::mccollect()
  expect_true(is.null(x))
})
