context("flatten")

testthat("flatten behaves", {
  x = data.table(
    id = 1:3,
    nested.atomic = list(1, 2, 3),
    nested.list = list(list(a = 1), list(a = 2), list(a = 33)),
    nested.2dlist = list(list(a = 1, b = 2), list(a = 1), list(b = 2)),
    nested.df = list(data.frame(a = 1, b = 2), data.frame(a = 1), data.frame(b = 2))
  )
  x = as.data.frame(x)

  col = "nested.atomic"
  res = flatten(x, col)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x), col.names = "unique", any.missing = FALSE)
  expect_set_equal(names(res), names(x))
  expect_numeric(res[[col]])

  col = "nested.list"
  res = flatten(x, col)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x), col.names = "unique", any.missing = FALSE)
  expect_set_equal(names(res), c(setdiff(names(x), col), "a"))
  expect_numeric(res[["a"]])

  col = "nested.list"
  res = flatten(x, col, sep = ".")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x), col.names = "unique", any.missing = FALSE)
  expect_set_equal(names(res), c(setdiff(names(x), col), "nested.list.a"))
  expect_numeric(res[["nested.list.a"]])

  col = "nested.2dlist"
  res = flatten(x, col)
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_set_equal(names(res), c(setdiff(names(x), col), "a", "b"))
  expect_numeric(res[["a"]])
  expect_numeric(res[["b"]])

  col = "nested.2dlist"
  res = flatten(x, col, sep = "_")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_set_equal(names(res), c(setdiff(names(x), col), "nested.2dlist_a", "nested.2dlist_b"))
  expect_numeric(res[["nested.2dlist_a"]])
  expect_numeric(res[["nested.2dlist_b"]])

  col = "nested.df"
  res = flatten(x, col, sep = "_")
  expect_data_table(res, nrow = nrow(x), ncol = ncol(x) + 1L, col.names = "unique", any.missing = TRUE)
  expect_set_equal(names(res), c(setdiff(names(x), col), "nested.df_a", "nested.df_b"))
  expect_numeric(res[["nested.df_a"]])
  expect_numeric(res[["nested.df_b"]])


})
