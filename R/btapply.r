#' @title Synchronous Apply Functions
#'
#' @description
#' This is a set of convenience functions as counterparts to the sequential
#' apply functions in base R: \code{btlapply} for \code{\link[base]{lapply}} and
#' \code{btmapply} for \code{link[base]{mapply}}.
#' Internally the jobs are created in a temporary registry (see \code{\link{makeTempRegistry}}
#' and \code{\link{batchMap}} is called on the input vector(s).
#' After \code{\link{waitForJobs}} terminated, the result is reduced into a list.
#' Because the result is only returned as soon as all jobs are terminated,
#' the execution is called synchronized (in contrast to the usual asynchronous execution on
#' batch systems).
#'
#' Note that these functions are one suitable for short and fail-safe operations
#' on batch system. If some jobs fail, you have to retrieve partial results from the
#' temporary registry directory yourself.
#'
#' @param X [\code{\link[base]{vector}}]\cr
#'   Vector to apply over.
#' @param fun [\code{function}]\cr
#'   Function to apply.
#' @param more.args [\code{list}]\cr
#'   Additional arguments passed to \code{fun}.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to \code{fun} (\code{btlapply}) or vectors to map over (\code{btmapply}).
#' @template reg
#' @return [\code{list}] List with the results of the function call.
#' @export
btlapply = function(X, fun, ..., reg = makeTempRegistry(make.default = FALSE)) {
  assertVector(X)
  assertFunction(fun)
  assertRegistry(reg, writeable = TRUE, strict = TRUE)

  ids = do.call(batchMap, list(X, fun = fun, reg = reg, more.args = list(...)))
  submitJobs(ids = ids, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  reduceResultsList(ids = ids, reg = reg)
}

#' @export
#' @rdname btlapply
btmapply = function(fun, ..., more.args = list(), reg = makeTempRegistry(make.default = FALSE)) {
  # TODO: use.names, simplify
  assertFunction(fun)
  assertRegistry(reg, writeable = TRUE, strict = TRUE)

  ids = do.call(batchMap, list(..., fun = fun, reg = reg, more.args = more.args))
  submitJobs(ids = ids, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  reduceResultsList(ids = ids, reg = reg)
}
