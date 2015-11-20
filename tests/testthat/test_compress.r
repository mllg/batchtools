context("compression")

test_that("compress option works", {
  file_compression = function(fn) {
    fp = file(file.path(reg$file.dir, "registry.rds"), open = "r")
    class = summary(fp)$class
    close(fp)
    class
  }
  reg = makeTempRegistry(make.default = FALSE)
  expect_true(file_compression(file.path(reg$file.dir, "registry.rds")) == "gzfile")
  ids = batchMap(identity, 1, reg = reg)
  silent({
    submitJobs(1, reg = reg)
    waitForJobs(reg = reg)
  })
  expect_true(file_compression(file.path(reg$file.dir, "results", "1.rds")) == "gzfile")

  withr::with_options(list(batchtools.compress = "xz"), {
    reg = makeTempRegistry(make.default = FALSE)
    expect_true(file_compression(file.path(reg$file.dir, "registry.rds")) == "xzfile")
    ids = batchMap(identity, 1, reg = reg)
    silent({
      submitJobs(1, reg = reg)
      waitForJobs(reg = reg)
    })
    expect_true(file_compression(file.path(reg$file.dir, "results", "1.rds")) == "xzfile")
  })
})
