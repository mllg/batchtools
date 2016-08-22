context("showLog/getLog")

test_that("showLog/getLog", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(reg = reg, function(x) print("GREPME"), 1:2)
  expect_error(showLog(id = 1, reg = reg), "not available")
  expect_error(readLog(id = 1, reg = reg), "not available")
  ids = chunkIds(1:2, n.chunks = 1, reg = reg)
  silent({
    submitJobs(reg = reg, ids = ids)
    waitForJobs(reg = reg)
  })
  lines = getLog(id = 1, reg = reg)
  expect_character(lines, min.len = 3L, any.missing = FALSE)
  expect_equal(sum(stri_detect_fixed(lines, "GREPME")), 1L)
  expect_true(any(stri_startswith_fixed(lines, "### [bt ")))
  expect_identical(sum(stri_endswith_fixed(lines, "[batchtools job.id=1]")), 2L)
  expect_false(any(stri_endswith_fixed(lines, "[batchtools job.id=2]")))

  lines = getLog(id = 2, reg = reg)
  expect_false(any(stri_endswith_fixed(lines, "[batchtools job.id=1]")))

  with_options(list(pager = function(files, header, title, delete.file) files), {
    x = showLog(id = 2, reg = reg)
    expect_equal(basename(x), "2.log")
    expect_equal(sum(stri_detect_fixed(readLines(x), "GREPME")), 1L)
  })

  expect_error(getLog(id = 1:2, reg = reg), "exactly")
  expect_error(getLog(id = 3, reg = reg), "exactly")
})
