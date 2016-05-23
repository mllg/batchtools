context("runOSCommand")

test_that("runOSCommand", {
  skip_on_os("windows")
  x = runOSCommand("ls", find.package("batchtools"))
  expect_list(x, names = "named", len = 2)
  expect_identical(x$exit.code, 0L)
  expect_true(all(c("DESCRIPTION", "NAMESPACE", "NEWS.md") %in% x$output))
})

test_that("command not found", {
  res = runOSCommand("notfoundcommand")
  expect_list(res, len = 2)
  expect_identical(res$exit.code, 127L)
  expect_identical(res$output, "command not found")
})
