context("findConfFile")

test_that("findConfFile", {
  d = tempdir()
  fn = file.path(d, "batchtools.conf.R")
  writeLines("hello", fn)
  prev = getwd()
  setwd(d)
  expect_equal(findConfFile(), npath(fn))
  setwd(prev)
})
