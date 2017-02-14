context("removeRegistry")

test_that("removeRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  expect_true(dir.exists(reg$file.dir))
  res = removeRegistry(0.01, reg = reg)
  expect_true(res)
  expect_false(dir.exists(reg$file.dir))
})

test_that("removeRegistry resets default registry", {
  prev = batchtools$default.registry
  reg = makeExperimentRegistry(file.dir = NA, make.default = TRUE)
  expect_is(batchtools$default.registry, "Registry")
  res = removeRegistry(0, reg = reg)
  expect_false(dir.exists(reg$file.dir))
  expect_null(batchtools$default.registry)
  batchtools$default.registry = prev
})
