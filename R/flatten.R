#' @title Convert a Nested Data Frames to a Flat Representation
#'
#' @description
#' Some functions (e.g., \code{\link{getJobPars}}, \code{\link{getJobResources}} or \code{\link{reduceResultsDataTable}}
#' return a \code{data.table} with columns of type \code{list}.
#' These columns can be unnested to create a flat data.frame, iff each row holds
#' \describe{
#'   \item{(a)}{a single atomic value (an atomic scalar),}
#'   \item{(b)}{a named list of atomic scalars, or}
#'   \item{(c)}{a \code{data.frame} with one row.}
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
    if (qtestr(xc, "a1", depth = 1L)) { # atomic scalars in list
      set(res, j = col, value = unlist(xc, use.names = FALSE))
    } else if (qtestr(xc, c("d+", "l+"), depth = 1L)) {
      if (fix.data.table) {
        xc.names = lapply(xc, names2)
        xc.unames = unique(unlist(nc.names))
        if (anyMissing(xc.unames))
          stopf("Unnamed items in column '%s'", col)
        xc.template = setNames(vector("list", length(xc.unames)), xc.unames)
        xc = lapply(xc, function(x) insert(xc.template, x))
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
