context("Export")

test_that("export works", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  x = batchExport(list(exported_obj = 42L), reg = reg)
  expect_data_table(x, nrow = 1, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_equal(x$name, "exported_obj")
  expect_file_exists(file.path(reg$file.dir, "exports", "exported_obj.rds"))
  loadRegistryDependencies(reg)
  expect_equal(get("exported_obj", envir = .GlobalEnv), 42L)

  x = batchExport(reg = reg)
  expect_data_table(x, nrow = 1, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_equal(x$name, "exported_obj")

  x = batchExport(unexport = "exported_obj", reg = reg)
  expect_data_table(x, nrow = 0, ncol = 2)
  expect_set_equal(names(x), c("name", "uri"))
  expect_false(file.exists(file.path(reg$file.dir, "exports", "exported_obj.rds")))

  x = batchExport(list(exported_obj = 43L), reg = reg)
  batchMap(function(x) exported_obj + x, 1L, reg = reg)
  submitAndWait(reg)
  expect_equal(loadResult(1, reg = reg), 44L)
  rm("exported_obj", envir = .GlobalEnv)
})
