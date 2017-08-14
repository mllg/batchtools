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

  fix.data.table = packageVersion("data.table") <= "1.10.4"
  for (col in cols) {
    xc = x[[col]]
    if (qtestr(xc, "a1", depth = 1L)) { # atomic in list
      set(res, j = col, value = unlist(xc, use.names = FALSE))
    } else {
      if (fix.data.table) {
        x.items = max(lengths(xc))
        x.names = unique(unlist(lapply(xc, names)))
        if (x.items > length(x.names)) {
          xc = lapply(xc, function(x) {
            i = which(is.na(names2(x)))
            setNames(x, replace(names(x), i, sprintf("V%i", seq_along(i))))
          })
        }
          # x.template = setNames(vector("list", length(x.names)), x.names)
          # xc = lapply(xc, function(x) insert(x.template, x))
      }

      new = rbindlist(xc, fill = TRUE, idcol = "..row")
      if (ncol(new) > 0L) {
        if (nrow(new) > uniqueN(new, by = "..row"))
          stopf("Cannot unnest column '%s'", col)
        if (!is.null(sep)) {
          nn = chsetdiff(names(new), "..row")
          setnames(new, nn, stri_paste(col, sep, nn))
        }
        res = merge(res, new, all.x = TRUE, by = "..row")
      }
    }
  }

  if (all(key(x) %chin% names(res)))
    setkeyv(res, key(x))
  res[, !"..row"]
}
