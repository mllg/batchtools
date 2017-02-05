context("removeRegistry")

test_that("removeRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  expect_true(dir.exists(reg$file.dir))
  res = removeRegistry(0, reg = reg)
  expect_true(res)
  expect_false(dir.exists(reg$file.dir))
})
