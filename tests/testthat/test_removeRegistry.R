test_that("removeRegistry", {
  reg = makeTestRegistry()
  expect_directory_exists(reg$file.dir)
  res = removeRegistry(0.01, reg = reg)
  expect_string(res)
  expect_false(fs::dir_exists(reg$file.dir))
})

test_that("removeRegistry resets default registry", {
  prev = batchtools$default.registry
  reg = makeTestExperimentRegistry(make.default = TRUE)
  expect_is(batchtools$default.registry, "Registry")
  res = removeRegistry(0, reg = reg)
  expect_false(fs::dir_exists(reg$file.dir))
  expect_null(batchtools$default.registry)
  batchtools$default.registry = prev
})
