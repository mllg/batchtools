x = iris
x$xxx = list(replicate(150, list(a = runif(1), b = rnorm(2), simplify = FALSE)))
col = "xxx"

flatten = function(x, cols = NULL, sep = NULL) {
  assertDataFrame(x)
  if (is.null(cols))
    cols = names(x)[vlapply(x, is.list)]
  assertNames(cols, subset.of = names(x))

  for (col in cols) {
    rbindlist(x[[col]],fill = TRUE)
  }
}
