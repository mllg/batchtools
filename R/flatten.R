#' @title Convert a Nested Data Frames to a Flat Representation
#'
#' @description
#' Some functions (e.g., \code{\link{getJobPars}}, \code{\link{getJobResources}} or \code{\link{reduceResultsDataTable}}
#' return a \code{data.table} with columns of type \code{list}.
#' These columns can be unnested, iff each row hols
#' \itemize{
#'   \item a single atomic value (an atomic scalar),
#'   \item a named list of atomic scalars, or
#'   \item a \code{data.frame} with one row.
#' }
#' The values will be transformed to a \code{data.frame} and \code{\link[base]{cbind}}-ed to the input data.frame \code{x}.
#'
#' @param x [\code{\link{data.frame}}]\cr
#'   Data frame to flatten.
#' @param cols [\code{character}]\cr
#'   Columns to consider for this operation. If set to \code{NULL} (default),
#'   will operate on all columns of type \dQuote{list}.
#' @param sep [\code{character(1)}]\cr
#'   If \code{NULL} (default), the column names of the additional columns will re-use the names
#'   of the inner \code{list}/\code{data.frame}.
#'   If you provide \code{sep}, the variable column name will be constructed as
#'   \dQuote{[column name of x][sep][inner name]}.
#' @return [\code{\link{data.table}}] with unnested columns.
#' @export
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
