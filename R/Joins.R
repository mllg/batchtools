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
  w = unique(x[y, on = "job.id", nomatch = 0L, which = TRUE, allow.cartesian = TRUE])
  x[w]
}

#' @rdname JoinTables
#' @export
ajoin = function(x, y) {
  x = castIds(x)
  y = castIds(y)
  setkeyv(x[!y, on = "job.id"], "job.id")[]
}

#' @rdname JoinTables
#' @param all.y [logical(1)]\cr
#'   Keep columns of \code{y} which are not in \code{x}?
#' @export
ujoin = function(x, y, all.y = FALSE) {
  assertFlag(all.y)
  x = castIds(x, ensure.copy = TRUE)
  y = castIds(y)

  cn = setdiff(names(y), "job.id")
  if (!all.y)
    cn = intersect(names(x), cn)
  if (length(cn) == 0L)
    return(x)
  x[y, cn := mget(sprintf("i.%s", cn)), on = "job.id", nomatch = 0L, with = FALSE][]
}
