context("findConfFile")

test_that("findConfFile", {
  expect_identical(findConfFile("youwontfineme"), character(0))

  d = tempdir()
  fn = file.path(d, "my.conf")
  writeLines("hello", fn)
  prev = getwd()
  setwd(d)
  expect_equal(findConfFile("my.conf"), fn)
  setwd(prev)
})
