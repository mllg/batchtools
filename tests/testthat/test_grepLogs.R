context("grepLogs")

reg = makeRegistry(file.dir = NA, make.default = FALSE)
ids = batchMap(reg = reg, function(x) {
  if (x == 1) {
    print("FOOBAR: AAA")
  } else if (x == 2) {
    cat("FOOBAR: BBB")
  } else {
    message("FOOBAR: CCC")
  }
  invisible(NULL)
}, x = 1:3)
ids$chunk = as.integer(c(1, 1, 2))
silent({
  submitJobs(reg = reg, ids = ids[1:3])
  waitForJobs(reg = reg)
})

test_that("readLogs", {
  expect_true(any(grepl("AAA", readLog(1, reg = reg))))
  expect_true(any(grepl("BBB", readLog(2, reg = reg))))
  expect_true(any(grepl("CCC", readLog(3, reg = reg))))
})

test_that("grepLogs", {
  expect_data_table(grepLogs(pattern = "FOOBAR", reg = reg), ncol = 2, key = "job.id")
  expect_equal(grepLogs(pattern = "FOOBAR", reg = reg)$job.id, 1:3)
  expect_equal(grepLogs(pattern = "XXX", reg = reg)$job.id, integer(0L))
  expect_equal(grepLogs(pattern = "", reg = reg)$job.id, 1:3)
  expect_equal(grepLogs(pattern = "AAA", reg = reg)$job.id, 1L)
  expect_equal(grepLogs(pattern = "BBB", reg = reg)$job.id, 2L)
  expect_equal(grepLogs(pattern = "CCC", reg = reg)$job.id, 3L)
})
