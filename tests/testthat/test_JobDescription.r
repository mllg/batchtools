context("JobDescription")

test_that("makeJobDescription", {
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))

  j = makeJobDescription(ids, resources = list(foo = 42), reg = reg)
  expect_environment(j, c("file.dir", "job.hash", "defs", "log.file", "packages", "resources", "uri", "work.dir"))

  expect_directory(j$file.dir)
  expect_string(j$job.hash)
  expect_true(is.data.table(j$defs))
  expect_string(j$log.file)
  expect_character(j$packages, any.missing = FALSE)
  expect_list(j$resources, names = "unique")
  expect_string(j$uri)
  expect_directory(j$work.dir)

  expect_output(j, "Collection")
})
