context("grepLogs")

test_that("grepLogs", {
  reg = makeTempRegistry(FALSE)
  ids = batchMap(reg = reg, function(x) print(sprintf("FOOBAR: %s", paste(rep.int(LETTERS[x], 3), collapse = ""))), x = 1:4)
  ids$chunk = as.integer(c(1, 1, 2, 3))
  silent({
    submitJobs(reg = reg, ids = ids[1:3])
    waitForJobs(reg = reg)
  })

  ids = findJobs(reg = reg)
  expect_equal(grepLogs(pattern = "FOOBAR", reg = reg), ids[1:3])
  expect_equal(grepLogs(pattern = "", reg = reg), ids[1:3])
  expect_equal(grepLogs(pattern = "XXX", reg = reg), ids[FALSE])
  expect_equal(grepLogs(pattern = "AAA", reg = reg), ids[1, ])
  expect_equal(grepLogs(pattern = "BBB", reg = reg), ids[2, ])
  expect_equal(grepLogs(pattern = "CCC", reg = reg), ids[3, ])
})
