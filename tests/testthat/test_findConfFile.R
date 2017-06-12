context("findConfFile")

test_that("findConfFile", {
  d = tempdir()
  fn = file.path(d, "batchtools.conf.R")
  writeLines("hello", fn)
  withr::with_dir(d,
    expect_equal(findConfFile(), normalizePath(fn))
  )
})
