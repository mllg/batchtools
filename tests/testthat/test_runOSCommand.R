context("runOSCommand")

test_that("runOSCommand", {
  skip_on_os(c("windows", "solaris")) # system2 is broken on solaris
  x = runOSCommand("ls", find.package("batchtools"))
  expect_list(x, names = "named", len = 4L)
  expect_names(names(x), permutation.of = c("sys.cmd", "sys.args", "exit.code", "output"))
  expect_identical(x$exit.code, 0L)
  expect_true(all(c("DESCRIPTION", "NAMESPACE", "NEWS.md") %chin% x$output))
})

test_that("command not found", {
  skip_on_os("solaris") # system2 is broken on solaris
  res = runOSCommand("notfoundcommand")
  expect_list(res, len = 4)
  expect_identical(res$exit.code, 127L)
  expect_identical(res$output, "command not found")
  expect_error(OSError("Command not found", res), pattern = "Command not found")
  expect_error(OSError("Command not found", res), pattern = "'notfoundcommand'")
  expect_error(OSError("Command not found", res), pattern = "exit code 127")
})

test_that("stdin", {
  skip_on_os(c("windows", "solaris")) # system2 is broken on solaris

  tf = tempfile()
  lines = letters
  writeLines(letters, con = tf)
  res = runOSCommand("cat", stdin = tf)
  expect_identical(res$exit.code, 0L)
  expect_identical(res$output, letters)
})
