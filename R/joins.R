#' @title Inner, Left, Right, Outer and Anti Join for Job Tables
#' @name JoinTables
#'
#' @description
#' These helper functions perform join operations on job tables.
#' They are basically one-liners with additional argument checks for sanity.
#' See \url{http://rpubs.com/ronasta/join_data_tables} for a overview of join operations in
#' data table or alternatively \pkg{dplyr}'s vignette on two table verbs.
#'
#' @param x [\code{\link{data.table}}]\cr
#'   Data table with key \dQuote{job.id} as returned by most functions in batchtools.
#' @param y [\code{\link{data.table}}]\cr
#'   Data table with key \dQuote{job.id} as returned by most functions in batchtools.
#' @return [\code{\link{data.table}}] with key \dQuote{job.id}.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @export
#' @examples
#' # create two tables for demonstration
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, x = 1:6, reg = reg)
#' x = getJobPars(reg = reg)
#' y = findJobs(x >= 2 & x <= 5, reg = reg)
#' y$extra.col = head(letters, nrow(y))
#'
#' # inner join: similar to intersect() on ids, keep all columns of x and y
#' ijoin(x, y)
#'
#' # left join: use all ids from x, keep all columns of x and y
#' ljoin(x, y)
#'
#' # right join: use all ids from y, keep all columns of x and y
#' rjoin(x, y)
#'
#' # outer join: similar to union() on ids, keep all columns of x and y
#' ojoin(x, y)
#'
#' # anti join: similar to setdiff() on ids, keep all columns of x
#' ajoin(x, y)
ijoin = function(x, y) {
  assertDataTable(x, key = "job.id")
  assertInteger(x$job.id, any.missing = FALSE, lower = 1L)
  assertDataTable(y, key = "job.id")
  assertInteger(y$job.id, any.missing = FALSE, lower = 1L)
  x[y, nomatch = 0L]
}

#' @rdname JoinTables
#' @export
ljoin = function(x, y) {
  assertDataTable(x, key = "job.id")
  assertInteger(x$job.id, any.missing = FALSE, lower = 1L)
  assertDataTable(y, key = "job.id")
  assertInteger(y$job.id, any.missing = FALSE, lower = 1L)
  y[x]
}

#' @rdname JoinTables
#' @export
rjoin = function(x, y) {
  assertDataTable(x, key = "job.id")
  assertInteger(x$job.id, any.missing = FALSE, lower = 1L)
  assertDataTable(y, key = "job.id")
  assertInteger(y$job.id, any.missing = FALSE, lower = 1L)
  x[y]
}

#' @rdname JoinTables
#' @export
ojoin = function(x, y) {
  assertDataTable(x, key = "job.id")
  assertInteger(x$job.id, any.missing = FALSE, lower = 1L)
  assertDataTable(y, key = "job.id")
  assertInteger(y$job.id, any.missing = FALSE, lower = 1L)
  merge(x, y, all = TRUE)
}

#' @rdname JoinTables
#' @export
ajoin = function(x, y) {
  assertDataTable(x, key = "job.id")
  assertInteger(x$job.id, any.missing = FALSE, lower = 1L)
  assertDataTable(y, key = "job.id")
  assertInteger(y$job.id, any.missing = FALSE, lower = 1L)
  x[!y]
}
