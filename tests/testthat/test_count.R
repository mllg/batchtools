context("count")

test_that("count", {
  expect_identical(count(1:3), 3L)
  expect_identical(count(integer(0L)), 0L)
  expect_identical(count(list()), 0L)
  expect_identical(count(c(TRUE, NA, FALSE)), 2L)
  expect_identical(count(c(1L, NA, 3L)), 2L)
  expect_identical(count(c(1., NA, 3.)), 2L)
  expect_identical(count(c("a", NA, "c")), 2L)
  expect_identical(count(list(1, NULL, 3)), 2L)
})
