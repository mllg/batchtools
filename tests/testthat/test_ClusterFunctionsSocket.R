context("cf socket")

test_that("cf socket", {
  skip_if_not_installed("snow")
  # skip_on_travis()

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsSocket(2)
  ids = batchMap(Sys.sleep, time = c(5, 5), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.5, reg = reg))
  })
  expect_data_table(findOnSystem(reg = reg), nrow = 0)
  expect_equal(findDone(reg = reg), findJobs(reg = reg))
})
