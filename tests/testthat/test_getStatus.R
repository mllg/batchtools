context("getStatus")

test_that("getStatus", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(i) if (i == 4) stop("4!") else i
  ids = batchMap(fun, i = 1:10, reg = reg)
  submitAndWait(reg, 1:5)

  stat = getStatus(reg = reg)
  expect_data_table(stat, any.missing = FALSE, types = "integer", nrows = 1L)

  expect_equal(stat$defined, 10L)
  expect_equal(stat$submitted, 5L)
  expect_equal(stat$started, 5L)
  expect_equal(stat$done, 4L)
  expect_equal(stat$error, 1L)
  expect_equal(stat$queued, 0L)
  expect_equal(stat$running, 0L)
  expect_equal(stat$system, 0L)
  expect_equal(stat$expired, 0L)

  expect_output(print(stat), "Status for 10 jobs")
})
