context("JobNames")

test_that("setJobNames", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg)

  expect_null(setJobNames(ids, letters[1:3], reg = reg))

  x = getJobNames(reg = reg)
  expect_data_table(x, ncol = 2, nrow = 3, key = "job.id")
  expect_identical(x$job.name, letters[1:3])
  expect_identical(reg$status$job.name, letters[1:3])

  expect_data_table(ijoin(getJobNames(1:2, reg = reg), getJobPars(reg = reg)),
    ncol = 3, nrow = 2, key = "job.id")

  jc = makeJobCollection(1, reg = reg)
  expect_identical(jc$job.name, "a")

  jc = makeJobCollection(1:3, reg = reg)
  expect_identical(jc$job.name, "a")
  expect_identical(jc$jobs$job.name, letters[1:3])

  expect_null(setJobNames(ids, rep(NA_character_, 3), reg = reg))
  x = getJobNames(reg = reg)
  expect_data_table(x, ncol = 2, nrow = 3, key = "job.id")
  expect_identical(x$job.name, rep(NA_character_, 3))

  jc = makeJobCollection(1:3, reg = reg)
  expect_identical(jc$job.name, jc$job.hash)
})
