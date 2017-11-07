#' @title Convert Nested Data Frames to a Flat Representation
#'
#' @description
#' Some functions (e.g., \code{\link{getJobPars}}, \code{\link{getJobResources}} or \code{\link{reduceResultsDataTable}}
#' return a \code{data.table} with columns of type \code{list}.
#' These columns can be unnested/unwrapped with this function.
#' The values will be transformed to a \code{data.frame} and \code{\link[base]{cbind}}-ed to the input data.frame \code{x},
#' replacing the original nested column.
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
#' @return [\code{\link{data.table}}] where nested columns are replaced with flattened columns.
#' @export
#' @examples
#' x = data.table(
#'   id = 1:3,
#'   values = list(list(a = 1, b = 3), list(a = 2, b = 2), list(a = 3))
#' )
#' flatten(x)
#' flatten(x, sep = ".")
flatten = function(x, cols = NULL, sep = NULL) {
  assertDataFrame(x)
  if (!is.data.table(x))
    x = as.data.table(x)

  if (is.null(cols)) {
    cols = names(x)[vlapply(x, is.list)]
  } else {
    assertNames(cols, "unique", subset.of = names(x))
    qassertr(x[, cols, with = FALSE], "l")
  }
  assertString(sep, null.ok = TRUE)

  res = data.table(..row = seq_row(x), key = "..row")
  for (col in cols) {
    xc = x[[col]]

    new.cols = lapply(seq_along(xc), function(i) {
      x = xc[[i]]
      if (is.null(x))
        return(list(..row = i))
      x = lapply(x, function(x) if (!qtest(x, c("l", "v1"))) list(x) else x)
      na = which(is.na(names2(x)))
      if (length(na) > 0L)
        names(x)[na] = sprintf("%s.%i", col, seq_along(na))
      x$..row = i
      x
    })
    new.cols = rbindlist(new.cols, fill = TRUE)

    if (ncol(new.cols) >= 2L) {
      if (!is.null(sep))
        setnames(new.cols, chsetdiff(names(new.cols), "..row"), stri_paste(col, chsetdiff(names(new.cols), "..row"), sep = sep))
      clash = chintersect(names(res), names(new.cols))
      if (length(clash) >= 2L)
        stopf("Name clash while flattening data.table: Duplicated column names: %s", stri_flatten(chsetdiff(clash, "..row")))
      res = rjoin(res, new.cols, by = "..row")
    }
  }

  for (col in chsetdiff(names(x), cols))
    set(res, j = col, value = x[[col]])
  res[, "..row" := NULL]
  setcolorder(res, c(chsetdiff(names(x), cols), chsetdiff(names(res), names(x))))

  kx = key(x)
  if (!is.null(kx) && all(kx %chin% names(res)))
    setkeyv(res, kx)
  res[]
}
