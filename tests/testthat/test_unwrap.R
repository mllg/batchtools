test_that("unwrap behaves", {
  x = data.table(
    id = 1:3,
    nested.list = list(list(a = 1), list(a = 2), list(a = 33)),
    nested.2dlist = list(list(a = 1, b = 2), list(a = 1), list(b = 2)),
    nested.df = list(data.frame(a = 1, b = 2), data.frame(a = 1), data.frame(b = 2)),
    multi.row = list(data.frame(a = 1:2, b = 1:2), data.frame(a = 3:4, b = 3:4), data.frame(a = 1:2, b = 3:4)),
    empty = list(NULL, NULL, NULL)
  )

  cols = "nested.list"
  res = unwrap(x, cols)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x), col.names = "unique", any.missing = FALSE)
  expect_equal(names(res), c("id", "nested.2dlist", "nested.df", "multi.row", "empty", "a"))
  expect_numeric(res[["a"]])

  cols = "nested.list"
  res = unwrap(x, cols, sep = ".")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x), col.names = "unique", any.missing = FALSE)
  expect_equal(names(res), c("id", "nested.2dlist", "nested.df", "multi.row", "empty", "nested.list.a"))
  expect_numeric(res[["nested.list.a"]])

  cols = "nested.2dlist"
  res = unwrap(x, cols)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_equal(names(res), c("id", "nested.list", "nested.df", "multi.row", "empty", "a", "b"))
  expect_numeric(res[["a"]])
  expect_numeric(res[["b"]])

  cols = "nested.2dlist"
  res = unwrap(x, cols, sep = "_")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_set_equal(names(res), c("id", "nested.list", "nested.2dlist_a", "nested.2dlist_b", "nested.df", "multi.row", "empty"))
  expect_numeric(res[["nested.2dlist_a"]])
  expect_numeric(res[["nested.2dlist_b"]])

  cols = "nested.df"
  res = unwrap(x, cols, sep = "_")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_set_equal(names(res), c("id", "nested.list", "nested.2dlist", "nested.df_a", "nested.df_b", "multi.row", "empty"))
  expect_numeric(res[["nested.df_a"]])
  expect_numeric(res[["nested.df_b"]])

  cols = "empty"
  res = unwrap(x, cols)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) - 1L, col.names = "unique", any.missing = TRUE)
  expect_equal(names(res), c("id", "nested.list", "nested.2dlist", "nested.df", "multi.row"))

  expect_error(unwrap(x), "Name clash")
  x = data.table(x = list(2, 3, 5), y = 1:3)
  res = unwrap(x)
  expect_data_table(res, nrow = 3, ncol = 2, col.names = "unique", any.missing = FALSE)
  expect_set_equal(names(res), c("y", "x.1"))
})
