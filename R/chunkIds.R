#' @title Chunk Jobs for Sequential Execution
#'
#' @description
#' Jobs can be partitioned into \dQuote{chunks} to be executed sequentially on the computational nodes.
#' Chunks are defined by providing a data frame with columns \dQuote{job.id} and \dQuote{chunk} (integer)
#' to \code{\link{submitJobs}}.
#' All jobs with the same chunk number will be grouped together on one node to form a single
#' computational job.
#'
#' The function \code{chunk} simply splits \code{x} into either a fixed number of groups, or
#' into a variable number of groups with a fixed number of maximum elements.
#'
#' The function \code{lpt} also groups \code{x} into a fixed number of chunks,
#' but uses the actual values of \code{x} in a greedy \dQuote{Longest Processing Time} algorithm.
#' As a result, the maximum sum of elements in minimized.
#'
#' \code{binpack} splits \code{x} into a variable number of groups whose sum of elements do
#' not exceed the upper limit provided by \code{chunk.size}.
#'
#' See examples of \code{\link{estimateRuntimes}} for an application of \code{binpack} and \code{lpt}.
#'
#' @param x [\code{numeric}]\cr
#'   For \code{chunk} an atomic vector (usually the \code{job.id}).
#'   For \code{binpack} and \code{lpt}, the weights to group.
#' @param chunk.size [\code{integer(1)}]\cr
#'   Requested chunk size for each single chunk.
#'   For \code{chunk} this is the number of elements in \code{x}, for \code{binpack} the size
#'   is determined by the sum of values in \code{x}.
#'   Mutually exclusive with \code{n.chunks}.
#' @param n.chunks [\code{integer(1)}]\cr
#'   Requested number of chunks.
#'   The function \code{chunk} distributes the number of elements in \code{x} evenly while
#'   \code{lpt} tries to even out the sum of elements in each chunk.
#'   If more chunks than necessary are requested, empty chunks are ignored.
#'   Mutually exclusive with \code{chunks.size}.
#' @param shuffle [\code{logical(1)}]\cr
#'   Shuffles the groups. Default is \code{TRUE}.
#' @return [\code{integer}] giving the chunk number for each element of \code{x}.
#' @seealso \code{\link{estimateRuntimes}}
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(2) }
#' ch = chunk(1:10, n.chunks = 2)
#' table(ch)
#'
#' ch = chunk(rep(1, 10), chunk.size = 2)
#' table(ch)
#'
#' set.seed(1)
#' x = runif(10)
#' ch = lpt(x, n.chunks = 2)
#' sapply(split(x, ch), sum)
#'
#' set.seed(1)
#' x = runif(10)
#' ch = binpack(x, 1)
#' sapply(split(x, ch), sum)
#'
#' # Job chunking
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(identity, 1:25, reg = tmp)
#'
#' ### Group into chunks with 10 jobs each
#' library(data.table)
#' ids[, chunk := chunk(job.id, chunk.size = 10)]
#' print(ids[, .N, by = chunk])
#'
#' ### Group into 4 chunks
#' ids[, chunk := chunk(job.id, n.chunks = 4)]
#' print(ids[, .N, by = chunk])
#'
#' ### Submit to batch system
#' submitJobs(ids = ids, reg = tmp)
#'
#' # Grouped chunking
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#' prob = addProblem(reg = tmp, "prob1", data = iris, fun = function(job, data) nrow(data))
#' prob = addProblem(reg = tmp, "prob2", data = Titanic, fun = function(job, data) nrow(data))
#' algo = addAlgorithm(reg = tmp, "algo", fun = function(job, data, instance, i, ...) problem)
#' prob.designs = list(prob1 = data.table(), prob2 = data.table(x = 1:2))
#' algo.designs = list(algo = data.table(i = 1:3))
#' addExperiments(prob.designs, algo.designs, repls = 3, reg = tmp)
#'
#' ### Group into chunks of 5 jobs, but do not put multiple problems into the same chunk
#' # -> only one problem has to be loaded per chunk, and only once because it is cached
#' ids = getJobTable(reg = tmp)[, .(job.id, problem, algorithm)]
#' ids[, chunk := chunk(job.id, chunk.size = 5), by = "problem"]
#' ids[, chunk := .GRP, by = c("problem", "chunk")]
#' dcast(ids, chunk ~ problem)
chunk = function(x, n.chunks = NULL, chunk.size = NULL, shuffle = TRUE) {
  assertAtomicVector(x)

  if (!xor(is.null(n.chunks), is.null(chunk.size)))
    stop("You must provide either 'n.chunks' (x)or 'chunk.size'")
  assertCount(n.chunks, positive = TRUE, null.ok = TRUE)
  assertCount(chunk.size, positive = TRUE, null.ok = TRUE)
  assertFlag(shuffle)

  n = length(x)
  if (n == 0L)
    return(integer(0L))
  if (is.null(n.chunks))
    n.chunks = (n %/% chunk.size + (n %% chunk.size > 0L))
  chunks = as.integer((seq.int(0L, n - 1L) %% min(n.chunks, n))) + 1L
  if (shuffle)
    chunks = sample(chunks)
  else
    chunks = sort(chunks)
  return(chunks)
}

#' @rdname chunk
#' @useDynLib batchtools c_lpt
#' @export
lpt = function(x, n.chunks = 1L) {
  assertNumeric(x, lower = 0, any.missing = FALSE, finite = TRUE)
  assertCount(n.chunks, positive = TRUE)

  .Call(c_lpt, as.numeric(x), order(x, decreasing = TRUE), as.integer(n.chunks))
}

#' @rdname chunk
#' @useDynLib batchtools c_binpack
#' @export
binpack = function(x, chunk.size = max(x)) {
  assertNumeric(x, lower = 0, any.missing = FALSE, finite = TRUE)
  assertNumber(chunk.size, lower = 0)
  if (length(x) == 0L)
    return(integer(0L))

  .Call(c_binpack, as.numeric(x), order(x, decreasing = TRUE), as.double(chunk.size))
}
