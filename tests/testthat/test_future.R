context("future.batchtools")

test_that("futures work", {
  skip_if_not_installed("future.batchtools")
  Sys.setenv(R_FUTURE_CACHE_PATH = file.path(tempdir(), ".future"))
  library("future")
  library("future.batchtools")
  plan(batchtools_local)

  pid %<-% { Sys.getpid() }
  expect_count(pid)
  expect_false(pid == Sys.getpid())
})
