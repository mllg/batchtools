#' @title Inner, Left, Right, Outer, Semi and Anti Join for Job Tables
#' @name JoinTables
#'
#' @description
#' These helper functions perform join operations on job tables.
#' They are basically one-liners with additional argument checks for sanity.
#' See \url{http://rpubs.com/ronasta/join_data_tables} for a overview of join operations in
#' data table or alternatively \pkg{dplyr}'s vignette on two table verbs.
#'
#' @param x [\code{\link{data.frame}} | \code{integer}]\cr
#'   Either a \code{\link[data.table]{data.table}}/\code{\link[base]{data.frame}} with integer column \dQuote{job.id}
#'   or an integer vector of job ids.
#' @param y [\code{\link{data.frame}} | \code{integer}]\cr
#'   Either a \code{\link[data.table]{data.table}}/\code{\link[base]{data.frame}} with integer column \dQuote{job.id}
#'   or an integer vector of job ids.
#' @return [\code{\link{data.table}}] with key \dQuote{job.id}.
#' @export
#' @examples
#' # Create two tables for demonstration
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, x = 1:6, reg = tmp)
#' x = getJobPars(reg = tmp)
#' y = findJobs(x >= 2 & x <= 5, reg = tmp)
#' y$extra.col = head(letters, nrow(y))
#'
#' # Inner join: similar to intersect() on ids, keep all columns of x and y
#' ijoin(x, y)
#'
#' # Left join: use all ids from x, keep all columns of x and y
#' ljoin(x, y)
#'
#' # Right join: use all ids from y, keep all columns of x and y
#' rjoin(x, y)
#'
#' # Outer join: similar to union() on ids, keep all columns of x and y
#' ojoin(x, y)
#'
#' # Semi join: similar to intersect() on ids, keep all columns of x
#' sjoin(x, y)
#'
#' # Anti join: similar to setdiff() on ids, keep all columns of x
#' ajoin(x, y)
ijoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  x[y, nomatch = 0L, on = "job.id"]
}

#' @rdname JoinTables
#' @export
ljoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  y[x, on = "job.id"]
}

#' @rdname JoinTables
#' @export
rjoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  x[y, on = "job.id"]
}

#' @rdname JoinTables
#' @export
ojoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  merge(x, y, all = TRUE, by = "job.id")
}

#' @rdname JoinTables
#' @export
sjoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  w = unique(x[y, on = "job.id", which = TRUE, allow.cartesian = TRUE])
  x[w]
}

#' @rdname JoinTables
#' @export
ajoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  setkeyv(x[!y, on = "job.id"], "job.id")[]
}

# FIXME: ujoin -> inserting/updating join?
