context("btlapply")

test_that("btlapply", {
  fun = function(x, y) x^y
  silent({
    expect_equal(btlapply(1:3, fun, y = 2), lapply(1:3, fun, y = 2))
    expect_equal(btmapply(fun, x = 1:3, y = 1:3), mapply(fun, x = 1:3, y = 1:3, SIMPLIFY = FALSE, USE.NAMES = FALSE))
  })
})
