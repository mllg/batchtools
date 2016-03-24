context("showLog")

test_that("showLog", {
  reg = makeTempRegistry(FALSE)
  batchMap(reg = reg, function(x) print("GREPME"), 1:2)
  expect_error(showLog(id = 1, reg = reg), "not available")
  expect_error(readLog(id = 1, reg = reg), "not available")
  ids = chunkIds(1:2, n.chunks = 1, reg = reg)
  silent({
    submitJobs(reg = reg, ids = ids)
    waitForJobs(reg = reg)
  })
  lines = readLog(id = 1, reg = reg)
  expect_character(lines, min.len = 3L, any.missing = FALSE, min.chars = 1L, pattern =  "^\\[job")
  expect_equal(sum(stri_detect_fixed(lines, "GREPME")), 1L)
  expect_true(all(stri_detect_regex(lines, "job\\((1|chunk)\\)")))
  expect_false(any(stri_detect_fixed(lines, "job(2)")))

  lines = readLog(id = 2, reg = reg)
  expect_true(all(stri_detect_regex(lines, "job\\((2|chunk)\\)")))
  expect_false(any(stri_detect_fixed(lines, "job(1)")))
})
