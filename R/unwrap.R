#' @title Unwrap Nested Data Frames
#'
#' @description
#' Some functions (e.g., \code{\link{getJobPars}}, \code{\link{getJobResources}} or \code{\link{reduceResultsDataTable}}
#' return a \code{data.table} with columns of type \code{list}.
#' These columns can be unnested/unwrapped with this function.
#' The contents of these columns  will be transformed to a \code{data.table} and \code{\link[base]{cbind}}-ed
#' to the input data.frame \code{x}, replacing the original nested column.
#'
#' @note
#' There is a name clash with function \code{flatten} in package \pkg{purrr}.
#' The function \code{flatten} is discouraged to use for this reason in favor of \code{unwrap}.
#'
#' @param x [\code{\link{data.frame}} | \code{\link[data.table]{data.table}}]\cr
#'   Data frame to flatten.
#' @param cols [\code{character}]\cr
#'   Columns to consider for this operation. If set to \code{NULL} (default),
#'   will operate on all columns of type \dQuote{list}.
#' @param sep [\code{character(1)}]\cr
#'   If \code{NULL} (default), the column names of the additional columns will re-use the names
#'   of the nested \code{list}/\code{data.frame}.
#'   This may lead to name clashes.
#'   If you provide \code{sep}, the variable column name will be constructed as
#'   \dQuote{[column name of x][sep][inner name]}.
#' @return [\code{\link{data.table}}].
#' @export
#' @examples
#' x = data.table(
#'   id = 1:3,
#'   values = list(list(a = 1, b = 3), list(a = 2, b = 2), list(a = 3))
#' )
#' unwrap(x)
#' unwrap(x, sep = ".")
unwrap = function(x, cols = NULL, sep = NULL) {
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
  extra.cols = chsetdiff(names(x), cols)
  if (length(extra.cols))
    res = cbind(res, x[, extra.cols, with = FALSE])

  for (col in cols) {
    xc = x[[col]]

    new.cols = lapply(seq_along(xc), function(i) {
      x = xc[[i]]
      if (is.null(x))
        return(list(..row = i))
      x = lapply(x, function(x) if (!qtest(x, c("l", "d", "v1"))) list(x) else x)
      na = which(is.na(names2(x)))
      if (length(na) > 0L)
        names(x)[na] = sprintf("%s.%i", col, seq_along(na))
      x$..row = i
      x
    })
    new.cols = rbindlist(new.cols, fill = TRUE)

    if (ncol(new.cols) > 1L) {
      if (nrow(new.cols) != nrow(x) || anyDuplicated(new.cols$..row) > 0L)
        stop("Some rows are unsuitable for unnesting. Flattening leads to data duplication.")
      new.cols$..row = NULL
      if (!is.null(sep))
        setnames(new.cols, names(new.cols), stri_paste(col, names(new.cols), sep = sep))
      clash = chintersect(names(res), names(new.cols))
      if (length(clash) > 0L)
        stopf("Name clash while unwrapping data.table: Duplicated column names: %s", stri_flatten(clash, ", "))
      res[, names(new.cols) := new.cols]
    }
  }

  res[, "..row" := NULL]
  kx = key(x)
  if (!is.null(kx) && all(kx %chin% names(res)))
    setkeyv(res, kx)
  res[]
}

#' @rdname unwrap
#' @export
flatten = function(x, cols = NULL, sep = NULL) { #nocov start
  "!DEBUG Call of soon-to-be deprecated function flatten. Use unwrap() instead!"
  unwrap(x, cols, sep)
} #nocov end
