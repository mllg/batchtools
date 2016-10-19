context("manual expensive tests")

test_that("rscimark", {
  skip_if_not_installed("rscimark")
  skip("manual test")

  reg = makeRegistry(file.dir = NA, make.default = FALSE, package = "rscimark")
  reg$cluster.functions = makeClusterFunctionsMulticore(4)
  batchMap(rscimark, minimum.time = rep(1, 5), reg = reg)
  submitJobs(reg = reg)
  waitForJobs(reg = reg)
  tab = getJobTable(reg = reg)
  expect_true(tab$started[5] >= min(tab$done[1:4]))

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsMulticore(4)
  batchMap(Sys.sleep, rep(3, 4), reg = reg)
  submitJobs(reg = reg)
  waitForJobs(reg = reg)
  tab = getJobTable(reg = reg)
  expect_true(all(as.numeric(diff(range(tab$started))) <= 2))
  expect_true(all(as.numeric(diff(range(tab$done))) <= 2))
})
