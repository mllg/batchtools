#' @title Synchronous Apply Functions
#'
#' @description
#' This is a set of functions acting as counterparts to the sequential popular apply functions in base R:
#' \code{btlapply} for \code{\link[base]{lapply}} and \code{btmapply} for \code{\link[base]{mapply}}.
#'
#' Internally, jobs are created using \code{\link{batchMap}} on the provided registry.
#' If no registry is provided, a temporary registry (see argument \code{file.dir} of \code{\link{makeRegistry}}) and \code{\link{batchMap}}
#' will be used.
#' After all jobs are terminated (see \code{\link{waitForJobs}}), the results are collected and returned as a list.
#'
#' Note that these functions are one suitable for short and fail-safe operations
#' on batch system. If some jobs fail, you have to retrieve partial results from the
#' registry directory yourself.
#'
#' @param X [\code{\link[base]{vector}}]\cr
#'   Vector to apply over.
#' @param fun [\code{function}]\cr
#'   Function to apply.
#' @param more.args [\code{list}]\cr
#'   Additional arguments passed to \code{fun}.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to \code{fun} (\code{btlapply}) or vectors to map over (\code{btmapply}).
#' @inheritParams submitJobs
#' @param n.chunks [\code{integer(1)}]\cr
#'   Passed to \code{\link{chunk}} before \code{\link{submitJobs}}.
#' @param chunk.size [\code{integer(1)}]\cr
#'   Passed to \code{\link{chunk}} before \code{\link{submitJobs}}.
#' @template reg
#' @return [\code{list}] List with the results of the function call.
#' @export
#' @examples
#' btlapply(1:3, function(x) x^2)
#' btmapply(function(x, y, z) x + y + z, x = 1:3, y = 1:3, more.args = list(z = 1), simplify = TRUE)
btlapply = function(X, fun, ..., resources = list(), n.chunks = NULL, chunk.size = NULL, reg = makeRegistry(file.dir = NA)) {
  assertVector(X)
  assertFunction(fun)
  assertRegistry(reg, class = "Registry", writeable = TRUE)

  ids = batchMap(fun, X, more.args = list(...), reg = reg)
  if (!is.null(n.chunks) || !is.null(chunk.size))
    ids$chunk = chunk(ids$job.id, n.chunks = n.chunks, chunk.size = chunk.size)
  submitJobs(ids = ids, resources = resources, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  reduceResultsList(ids = ids, reg = reg)
}

#' @export
#' @param simplify [\code{logical(1)}]\cr
#'   Simplify the results using \code{\link[base]{simplify2array}}?
#' @param use.names [\code{logical(1)}]\cr
#'   Use names of the input to name the output?
#' @rdname btlapply
btmapply = function(fun, ..., more.args = list(), simplify = FALSE, use.names = TRUE, resources = list(), n.chunks = NULL, chunk.size = NULL, reg = makeRegistry(file.dir = NA)) {
  assertFunction(fun)
  assertFlag(simplify)
  assertFlag(use.names)
  assertRegistry(reg, class = "Registry", writeable = TRUE)

  ids = batchMap(fun, ..., more.args = more.args, reg = reg)
  if (!is.null(n.chunks) || !is.null(chunk.size))
    ids$chunk = chunk(ids$job.id, n.chunks = n.chunks, chunk.size = chunk.size)
  submitJobs(ids = ids, resources = resources, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  res = reduceResultsList(ids = ids, reg = reg)

  if (use.names) {
    x = head(list(...), 1L)
    if (length(x) > 0L) {
      x = x[[1L]]
      if (is.null(names(x))) {
        if(is.character(x))
          names(res) = x
      } else {
        names(res) = names(x)
      }
    }
  }

  if (simplify) simplify2array(res) else res
}
