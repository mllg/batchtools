#' @title Inner, Left, Right, Outer, Semi and Anti Join for Data Tables
#' @name JoinTables
#'
#' @description
#' These helper functions perform join operations on data tables.
#' Most of them are basically one-liners.
#' See \url{https://rpubs.com/ronasta/join_data_tables} for a overview of join operations in
#' data table or alternatively \pkg{dplyr}'s vignette on two table verbs.
#'
#' @param x [\code{\link{data.frame}}]\cr
#'   First data.frame to join.
#' @param y [\code{\link{data.frame}}]\cr
#'   Second data.frame to join.
#' @param by [\code{character}]\cr
#'   Column name(s) of variables used to match rows in \code{x} and \code{y}.
#'   If not provided, a heuristic similar to the one described in the \pkg{dplyr} vignette is used:
#'   \enumerate{
#'     \item If \code{x} is keyed, the existing key will be used if \code{y} has the same column(s).
#'     \item If \code{x} is not keyed, the intersect of common columns names is used if not empty.
#'     \item Raise an exception.
#'   }
#'   You may pass a named character vector to merge on columns with different names in \code{x} and
#'   \code{y}: \code{by = c("x.id" = "y.id")} will match \code{x}'s \dQuote{x.id} column with \code{y}\'s
#'   \dQuote{y.id} column.
#' @return [\code{\link{data.table}}] with key identical to \code{by}.
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' # Create two tables for demonstration
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, x = 1:6, reg = tmp)
#' x = getJobPars(reg = tmp)
#' y = findJobs(x >= 2 & x <= 5, reg = tmp)
#' y$extra.col = head(letters, nrow(y))
#'
#' # Inner join: similar to intersect(): keep all columns of x and y with common matches
#' ijoin(x, y)
#'
#' # Left join: use all ids from x, keep all columns of x and y
#' ljoin(x, y)
#'
#' # Right join: use all ids from y, keep all columns of x and y
#' rjoin(x, y)
#'
#' # Outer join: similar to union(): keep all columns of x and y with matches in x or y
#' ojoin(x, y)
#'
#' # Semi join: filter x with matches in y
#' sjoin(x, y)
#'
#' # Anti join: filter x with matches not in y
#' ajoin(x, y)
#'
#' # Updating join: Replace values in x with values in y
#' ujoin(x, y)
ijoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  setKey(x[y, nomatch = 0L, on = by], by)
}

#' @rdname JoinTables
#' @export
ljoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  setKey(y[x, on = by], by)
}

#' @rdname JoinTables
#' @export
rjoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  setKey(x[y, on = by], by)
}

#' @rdname JoinTables
#' @export
ojoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  res = if (is.null(names(by)))
    merge(x, y, all = TRUE, by = by)
  else
    merge(x, y, all = TRUE, by.x = names(by), by.y = by)

  setKey(res, by)
}

#' @rdname JoinTables
#' @export
sjoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  w = unique(x[y, on = by, nomatch = 0L, which = TRUE, allow.cartesian = TRUE])
  setKey(x[w], by)
}

#' @rdname JoinTables
#' @export
ajoin = function(x, y, by = NULL) {
  x = as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  setKey(x[!y, on = by], by)
}

#' @rdname JoinTables
#' @param all.y [logical(1)]\cr
#'   Keep columns of \code{y} which are not in \code{x}?
#' @export
ujoin = function(x, y, all.y = FALSE, by = NULL) {
  assertFlag(all.y)
  x = if (is.data.table(x)) copy(x) else as.data.table(x)
  y = as.data.table(y)
  by = guessBy(x, y, by)

  cn = chsetdiff(names(y), by)
  if (!all.y)
    cn = chintersect(names(x), cn)
  if (length(cn) == 0L)
    return(x)

  expr = parse(text = stri_join("`:=`(", stri_flatten(sprintf("%1$s=i.%1$s", cn), ","), ")"))
  setKey(x[y, eval(expr), on = by], by)
}

guessBy = function(x, y, by = NULL) {
  assertDataFrame(x, min.cols = 1L)
  assertDataFrame(y, min.cols = 1L)

  if (is.null(by)) {
    res = key(x)
    if (!is.null(res) && all(res %chin% names(y)))
      return(res)

    res = chintersect(names(x), names(y))
    if (length(res) > 0L)
      return(res)
    stop("Unable to guess columns to match on. Please specify them explicitly or set keys beforehand.")
  } else {
    if (is.null(names(by))) {
      assertSubset(by, names(x))
    } else {
      assertSubset(names(by), names(x))
    }
    assertSubset(by, names(y))
    return(by)
  }
}

setKey = function(res, by) {
  by = names(by) %??% unname(by)
  if (!identical(key(res), by))
    setkeyv(res, by)
  res[]
}
