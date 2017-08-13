if (FALSE) {
  x = data.table(a = 1, col1 = replicate(5, list(a = runif(1)), simplify = FALSE))
}

flatten = function(x, cols = NULL, sep = NULL) {
  assertDataFrame(x)
  if (is.null(cols)) {
    cols = names(x)[vlapply(x, is.list)]
  } else {
    assertNames(cols, "unique", subset.of = names(x))
  }

  res = data.table(..row = seq_row(x), key = "..row")
  for (col in chsetdiff(names(x), cols))
    set(res, j = col, value = x[[col]])

  for (col in cols) {
    new = rbindlist(x[[col]], fill = TRUE, idcol = "..row")
    if (ncol(new) > 0L) {
      if (nrow(new) > uniqueN(new, by = "..row"))
        stopf("Cannot unnest column '%s'", col)
      if (!is.null(sep))
        setnames(new, names(new), stri_paste(col, sep, names(new)))
      res = merge(res, new, all.x = TRUE, by = "..row")
    }
  }

  if (all(key(x) %chin% names(res)))
    setkeyv(res, key(x))
  res[, !"..row"]
}
