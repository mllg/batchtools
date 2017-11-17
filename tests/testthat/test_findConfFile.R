context("findConfFile")

test_that("findConfFile", {
  d = tempdir()
  fn = fp(d, "batchtools.conf.R")
  file.create(fn)
  withr::with_dir(d,
    expect_equal(findConfFile(), normalizePath(fn, winslash = "/"))
  )
  withr::with_envvar(list(R_BATCHTOOLS_SEARCH_PATH = d),
    expect_equal(findConfFile(), normalizePath(fn, winslash = "/"))
  )
  file.remove(fn)
})
