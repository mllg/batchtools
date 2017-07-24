context("Export")

test_that("export works", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  x = batchExport(list(exported_obj = 42L), reg = reg)
  expect_data_table(x, nrow = 1, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_equal(x$name, "exported_obj")
  expect_file_exists(fp(reg$file.dir, "exports", mangle("exported_obj")))
  withr::with_dir(reg$work.dir, loadRegistryDependencies(reg))
  expect_equal(get("exported_obj", envir = .GlobalEnv), 42L)

  x = batchExport(reg = reg)
  expect_data_table(x, nrow = 1, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_equal(x$name, "exported_obj")

  x = batchExport(unexport = "exported_obj", reg = reg)
  expect_data_table(x, nrow = 0, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_false(file.exists(fp(reg$file.dir, "exports", mangle("exported_obj"))))

  x = batchExport(list(exported_obj = 43L), reg = reg)
  batchMap(function(x) exported_obj + x, 1L, reg = reg)
  submitAndWait(reg)
  expect_equal(loadResult(1, reg = reg), 44L)
  rm("exported_obj", envir = .GlobalEnv)
})


test_that("export works with funny variable names", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  x = batchExport(list(`%bla%` = function(x, y, ...) 42), reg = reg)
  expect_data_table(x, nrow = 1, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_equal(x$name, "%bla%")
  expect_file_exists(fp(reg$file.dir, "exports", mangle("%bla%")))
  withr::with_dir(reg$work.dir, loadRegistryDependencies(reg))
  expect_function(get("%bla%", envir = .GlobalEnv))
  expect_equal(1 %bla% 2, 42)

  x = batchExport(unexport = "%bla%", reg = reg)
  expect_data_table(x, nrow = 0, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_false(file.exists(fp(reg$file.dir, "exports", mangle("%bla%"))))

  rm("%bla%", envir = .GlobalEnv)
})
