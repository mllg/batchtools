context("expensive tests")

test_that("rscimark", {
  skip_if_not_installed("rscimark")
  skip("manual test")
  reg = makeRegistry(file.dir = NA, make.default = FALSE, package = "rscimark")
  batchMap(rscimark, minimum.time = rep(1, 5), reg = reg)
  submitJobs(reg = reg)

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(Sys.sleep, rep(3, 4), reg = reg)
  submitJobs(reg = reg)
  getJobTable(reg = reg)
})
