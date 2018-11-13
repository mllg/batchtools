context("grepLogs")

silent({
  reg = makeTestRegistry()
  ids = batchMap(reg = reg, function(x) {
    if (x == 1) {
      print("FOOBAR: AAA")
    } else if (x == 2) {
      cat("FOOBAR: BBB")
    } else {
      if (identical(Sys.getenv("TESTTHAT"), "true")) {
        # testthat uses muffle restarts which breaks our internal
        # sink() somehow.
        # https://github.com/r-lib/testthat/issues/460
        cat("FOOBAR: CCC", file = stderr())
      } else {
        message("FOOBAR: CCC")
      }
    }
    invisible(NULL)
  }, x = 1:5)
  ids$chunk = as.integer(c(1, 1, 2, 3, 4))
  submitAndWait(reg, ids[1:4])
})

test_that("grepLogs", {
  expect_true(any(grepl("AAA", getLog(1, reg = reg))))
  expect_true(any(grepl("BBB", getLog(2, reg = reg))))
  expect_true(any(grepl("CCC", getLog(3, reg = reg))))
  expect_false(any(grepl("AAA", getLog(2, reg = reg))))

  expect_data_table(grepLogs(pattern = "FOOBAR", reg = reg), ncol = 2, key = "job.id")
  expect_equal(grepLogs(pattern = "FOOBAR", reg = reg)$job.id, 1:4)
  expect_equal(grepLogs(pattern = "XXX", reg = reg)$job.id, integer(0L))
  expect_error(grepLogs(pattern = "", reg = reg), "at least")
  expect_error(grepLogs(pattern = NA, reg = reg), "not be NA")
  expect_equal(grepLogs(pattern = "AAA", reg = reg)$job.id, 1L)
  expect_equal(grepLogs(pattern = "BBB", reg = reg)$job.id, 2L)
  expect_equal(grepLogs(pattern = "CCC", reg = reg)$job.id, 3:4)

  expect_equal(grepLogs(pattern = "aaa", reg = reg)$job.id, integer(0L))
  expect_equal(grepLogs(pattern = "aaa", ignore.case = TRUE, reg = reg)$job.id, 1L)

  expect_data_table(grepLogs(pattern = "F..BAR", reg = reg), ncol = 2, nrow = 4, key = "job.id")
  expect_data_table(grepLogs(pattern = "F..BAR", fixed = TRUE, reg = reg), ncol = 2, nrow = 0, key = "job.id")

  expect_data_table(grepLogs(1:2, pattern = "CCC", reg = reg), nrow = 0, ncol = 2)
  expect_data_table(grepLogs(5, pattern = "CCC", reg = reg), nrow = 0, ncol = 2)
})
