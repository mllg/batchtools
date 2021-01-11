test_that("futures work", {
  skip_if_not_installed("future.batchtools")
  path = Sys.getenv("R_FUTURE_CACHE_PATH")
  if (!nzchar(path))
    Sys.setenv(R_FUTURE_CACHE_PATH = fs::path(fs::path_temp(), ".future"))

  library("future")
  library("future.batchtools")
  plan(batchtools_local)

  pid %<-% { Sys.getpid() }
  expect_count(pid)
  expect_false(pid == Sys.getpid())
})
