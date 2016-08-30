context("cf multicore / cf socket")

test_that("cf multicore", {
  skip_on_os("windows")

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(2)
  ids = batchMap(Sys.sleep, time = c(5, 5), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.5, reg = reg))
    expect_data_table(findOnSystem(reg = reg), nrow = 0)
    expect_equal(findDone(reg = reg), findJobs(reg = reg))
  })
})

test_that("cf socket", {
  skip_if_not_installed("snow")
  skip_on_travis()

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsSocket(2)
  ids = batchMap(Sys.sleep, time = c(5, 5), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.5, reg = reg))
    expect_data_table(findOnSystem(reg = reg), nrow = 0)
    expect_equal(findDone(reg = reg), findJobs(reg = reg))
  })
})

test_that("Multicore cleans up finished processes", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(Sys.sleep, rep(0.8, 8), reg = reg)
  p = Multicore$new(4)

  for (i in 1:4) {
    p$spawn(makeJobCollection(i, reg = reg))
  }
  expect_data_table(p$procs, ncol = 2)
  expect_integer(p$procs$pid, len = 4L, any.missing = FALSE, lower = 1L)
  expect_character(p$procs$hash, len = 4L, any.missing = FALSE, min.char = 1L)
  Sys.sleep(1.5)
  p$spawn(makeJobCollection(5L, reg = reg))
  expect_character(p$procs$hash, len = 1L, any.missing = FALSE, min.char = 1L)
  p$collect(3)
  x = parallel::mccollect()
  expect_true(is.null(x) || length(filterNull(x)) == 0L)
})
