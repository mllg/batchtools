context("findConfFile")

test_that("findConfFile", {
  d = tempdir()
  fn = fp(d, "batchtools.conf.R")
  writeLines("hello", fn)
  withr::with_dir(d,
    expect_equal(findConfFile(), normalizePath(fn, winslash = "/"))
  )
  file.remove(fn)
})
