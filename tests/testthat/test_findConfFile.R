test_that("findConfFile", {
  d = fs::path_real(fs::path_temp())
  fn = fs::path(d, "batchtools.conf.R")
  fs::file_create(fn)
  withr::with_dir(d,
    expect_equal(findConfFile(), fs::path_abs(fn))
  )
  withr::with_envvar(list(R_BATCHTOOLS_SEARCH_PATH = d),
    expect_equal(findConfFile(), fs::path_abs(fn))
  )
  fs::file_delete(fn)
})
