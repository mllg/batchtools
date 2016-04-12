context("btlapply")

test_that("btlapply", {
  fun = function(x, y) x^y
  silent({
    expect_equal(btlapply(1:3, fun, y = 2), lapply(1:3, fun, y = 2))
  })
})

test_that("btmapply", {
  fun = function(x, y) x
  x = letters[1:3]
    expect_equal(silent(btmapply(fun, x = x, y = 1:3, use.names = FALSE)), mapply(fun, x = x, y = 1:3, SIMPLIFY = FALSE, USE.NAMES = FALSE))
    expect_equal(silent(btmapply(fun, x = x, y = letters[1:3], use.names = FALSE, simplify = TRUE)), mapply(fun, x = x, y = 1:3, SIMPLIFY = TRUE, USE.NAMES = FALSE))
    expect_equal(silent(btmapply(fun, x = x, y = letters[1:3], use.names = TRUE, simplify = TRUE)), mapply(fun, x = x, y = 1:3, SIMPLIFY = TRUE, USE.NAMES = TRUE))
})
