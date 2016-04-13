#' @title Synchronous Apply Functions
#'
#' @description
#' This is a set of convenience functions as counterparts to the sequential
#' apply functions in base R: \code{btlapply} for \code{\link[base]{lapply}} and
#' \code{btmapply} for \code{link[base]{mapply}}.
#' Internally the jobs are created in a temporary registry (see argument \code{file.dir} of
#' \code{\link{makeRegistry}}) and \code{\link{batchMap}} is called on the input vector(s).
#' After \code{\link{waitForJobs}} terminated, the result is reduced into a list.
#' Because the result is only returned as soon as all jobs are terminated,
#' the execution is called synchronized (in contrast to the usual asynchronous execution on
#' classical batch systems).
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
#' @template reg
#' @return [\code{list}] List with the results of the function call.
#' @export
btlapply = function(X, fun, ..., reg = makeRegistry(file.dir = NA)) {
  assertVector(X)
  assertFunction(fun)
  assertRegistry(reg, writeable = TRUE, strict = TRUE)

  ids = do.call(batchMap, list(X, fun = fun, reg = reg, more.args = list(...)))
  submitJobs(ids = ids, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  reduceResultsList(ids = ids, reg = reg)
}

#' @export
#' @param simplify [\code{logical(1)}]\cr
#'   Simplify the results using \code{\link[base]{simplify2array}}?
#' @param use.names [\code{logical(1)}]\cr
#'   Use names of the input to name the output?
#' @rdname btlapply
btmapply = function(fun, ..., more.args = list(), simplify = FALSE, use.names = TRUE, reg = makeRegistry(file.dir = NA)) {
  assertFunction(fun)
  assertFlag(simplify)
  assertFlag(use.names)
  assertRegistry(reg, writeable = TRUE, strict = TRUE)

  dots = list(...)
  ids = do.call(batchMap, c(dots, list(fun = fun, reg = reg, more.args = more.args)))
  submitJobs(ids = ids, reg = reg)
  waitForJobs(ids = ids, reg = reg)
  res = reduceResultsList(ids = ids, reg = reg)

  if (use.names) {
    if (use.names && length(dots)) {
      if (is.null(names(dots[[1L]]))) {
        if(is.character(dots[[1L]]))
          names(res) = dots[[1L]]
      } else {
        names(res) = names(dots[[1L]])
      }
    }
  }

  if (simplify) simplify2array(res) else res
}
