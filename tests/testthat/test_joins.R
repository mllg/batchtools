context("joins")

test_that("joins", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, x = 1:6, reg = reg)
  x = getJobPars(reg = reg)[1:5]
  y = findJobs(x >= 2 & x <= 5, reg = reg)
  y$extra.col = head(letters, nrow(y))

  res = ijoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)

  res = ljoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))

  res = rjoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)

  res = rjoin(y, x)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))

  res = ojoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))

  res = sjoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)

  res = ajoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 1L)

  res = ijoin(x, 2:4)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:4)

  res = ijoin(2:4, x)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:4)

  res = ajoin(as.data.frame(x), y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 1L)
})
