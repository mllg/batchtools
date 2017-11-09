context("getSleepFunction")

test_that("getSleepFunction", {
  reg = makeTestRegistry()
  f = getSleepFunction(reg, NULL)
  expect_function(f)
  expect_true(any(grepl("Sys.sleep", as.character(body(f)))))

  f = getSleepFunction(reg, 99)
  expect_function(f)
  expect_true(any(grepl("Sys.sleep", as.character(body(f)))))

  f = getSleepFunction(reg, function(x) x^2)
  expect_function(f)
  expect_true(any(grepl("Sys.sleep", as.character(body(f)))))
})
