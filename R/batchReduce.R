#' @title Reduce Operation for Batch Systems
#'
#' @description
#' A parallel and asynchronous \code{\link[base]{Reduce}} for batch systems.
#' Note that this function only defines the computational jobs.
#' Each job reduces a certain number of elements on one slave.
#' The actual computation is started with \code{\link{submitJobs}}.
#' Results and partial results can be collected with \code{\link{reduceResultsList}}, \code{\link{reduceResults}} or
#' \code{\link{loadResult}}.
#'
#' @param fun [\code{function(aggr, x, ...)}]\cr
#'   Function to reduce \code{xs} with.
#' @param xs [\code{vector}]\cr
#'   Vector to reduce.
#' @param init [ANY]\cr
#'   Initial object for reducing. See \code{\link[base]{Reduce}}.
#' @param chunks [\code{integer(length(xs))}]\cr
#'   Group for each element of \code{xs}. Can be generated with \code{\link{chunk}}.
#' @param more.args [\code{list}]\cr
#'   A list of additional arguments passed to \code{fun}.
#' @template reg
#' @return [\code{\link{data.table}}] with ids of added jobs stored in column \dQuote{job.id}.
#' @export
#' @seealso \code{\link{batchMap}}
#' @examples
#' # define function to reduce on slave, we want to sum a vector
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' xs = 1:100
#' f = function(aggr, x) aggr + x
#'
#' # sum 20 numbers on each slave process, i.e. 5 jobs
#' chunks = chunk(xs, chunk.size = 5)
#' batchReduce(fun = f, 1:100, init = 0, chunks = chunks, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # now reduce one final time on master
#' reduceResults(fun = function(aggr, job, res) f(aggr, res), reg = tmp)
batchReduce = function(fun, xs, init = NULL, chunks = seq_along(xs), more.args = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, class = "Registry", writeable = TRUE)
  if (nrow(reg$defs) > 0L)
    stop("Registry must be empty")
  assertFunction(fun, c("aggr", "x"))
  assertAtomicVector(xs)
  assertIntegerish(chunks, len = length(xs), any.missing = FALSE, lower = 0L)
  assertList(more.args, names = "strict")

  more.args = c(more.args, list(..fun = fun, ..init = init))
  batchMap(batchReduceWrapper, unname(split(xs, chunks)), more.args = more.args, reg = reg)
}

batchReduceWrapper = function(xs.block, ..fun, ..init, ...) {
  fun = function(aggr, x) ..fun(aggr, x, ...)
  Reduce(fun, xs.block, init = ..init)
}
