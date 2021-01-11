test_that("joins", {
  reg = makeTestRegistry()
  batchMap(identity, x = 1:6, reg = reg)
  x = unwrap(getJobPars(reg = reg)[1:5])
  y = findJobs(x >= 2 & x <= 5, reg = reg)
  y$extra.col = head(letters, nrow(y))

  res = ijoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)
  expect_copied(res, x)

  res = ljoin(as.data.frame(x), y)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))
  expect_copied(res, x)

  res = rjoin(as.data.frame(x), y)
  expect_data_table(res, key = "job.id", ncol = 3, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)
  expect_copied(res, x)

  res = rjoin(y, x)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))
  expect_copied(res, x)

  res = ojoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_true(anyMissing(res$extra.col))
  expect_copied(res, x)

  res = sjoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)
  expect_copied(res, x)

  res = sjoin(y, x)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:5)
  expect_copied(res, x)

  res = ajoin(x, y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 1L)
  expect_copied(res, x)

  res = ijoin(x, data.frame(job.id = 2:4))
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:4)
  expect_copied(res, x)

  res = ijoin(data.frame(job.id = 2:4), x)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 2:4)
  expect_copied(res, x)

  res = ajoin(as.data.frame(x), y)
  expect_data_table(res, key = "job.id", ncol = 2, any.missing = FALSE)
  expect_identical(res$job.id, 1L)
  expect_copied(res, x)

  res = ujoin(x, y)
  expect_equivalent(res, x)
  expect_copied(res, x)

  yy = copy(y)
  yy$x = 10:13
  res = ujoin(x, yy)
  expect_data_table(res, key = "job.id", ncol = ncol(x), any.missing = FALSE)
  expect_identical(res$job.id, 1:5)
  expect_identical(res$x, c(1L, 10:13))
  expect_copied(res, x)

  res = ujoin(x, yy, all.y = TRUE)
  expect_data_table(res, key = "job.id", ncol = 3)
  expect_identical(res$job.id, 1:5)
  expect_identical(res$x, c(1L, 10:13))
  expect_identical(res$extra.col, c(NA, letters[1:4]))
  expect_copied(res, x)
})

test_that("guessBy", {
  x = data.frame(id = 1:3, x = 1:3)
  y = data.frame(jid = 1:3, y = 3:1)

  expect_error(guessBy(x, y), "explicitly")
  expect_error(guessBy(x, y, by = "id"), "subset of")

  by = guessBy(x, y, by = c(id = "jid"))
  expect_equal(unname(by), "jid")
  expect_equal(names(by), "id")

  y$id = y$jid
  by = guessBy(x, y, by = "id")
  expect_equal(unname(by), "id")
  expect_equal(names(by), NULL)
})
