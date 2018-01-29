context("showLog/getLog")

test_that("showLog/getLog", {
  reg = makeTestRegistry()
  batchMap(function(x) print("GREPME"), 1:2, reg = reg)
  expect_error(showLog(id = 1, reg = reg), "not available")
  expect_error(readLog(id = data.table(job.id = 1L), reg = reg), "not available")
  submitAndWait(reg)

  lines = getLog(id = 1, reg = reg)
  expect_character(lines, min.len = 3L, any.missing = FALSE)
  expect_equal(sum(stri_detect_fixed(lines, "GREPME")), 1L)
  expect_true(any(stri_startswith_fixed(lines, "### [bt")))
  expect_identical(sum(stri_endswith_fixed(lines, "[batchtools job.id=1]")), 2L)
  expect_false(any(stri_endswith_fixed(lines, "[batchtools job.id=2]")))

  lines = getLog(id = 2, reg = reg)
  expect_false(any(stri_endswith_fixed(lines, "[batchtools job.id=1]")))

  withr::with_options(list(pager = function(files, header, title, delete.file) files), {
    x = showLog(id = 2, reg = reg)
    expect_equal(fs::path_file(x), "2.log")
    expect_equal(sum(stri_detect_fixed(readLines(x), "GREPME")), 1L)
  })

  expect_error(getLog(id = 1:2, reg = reg), "exactly")
  expect_error(getLog(id = 3, reg = reg), "exactly")
})

test_that("empty log files", {
  reg = makeTestRegistry()
  batchMap(identity, 1, reg = reg)
  submitAndWait(reg)

  # overwrite log file
  log.file = getLogFiles(reg, 1)
  fs::file_delete(log.file)
  fs::file_create(log.file)

  x = readLog(data.table(job.id = 1), reg = reg)
  expect_data_table(x, ncol = 2, nrow = 0, index = "job.id")

  expect_equal(getLog(1, reg = reg), character(0L))
})
