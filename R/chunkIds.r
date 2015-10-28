#' @title Chunk Jobs for Sequential Execution
#'
#' @description
#' Partition jobs into \dQuote{chunks} which will be executed together on the nodes.
#'
#' Chunks are submitted via \code{\link{submitJobs}} by simply providing
#' job ids as a table with an additional column \dQuote{chunk}.
#' All jobs with the same chunk number will be grouped together on one node as a single
#' computational job.
#'
#' @templateVar ids.default all
#' @template ids
#' @param chunk.size [\code{integer(1)}]\cr
#'   Requested number of elements in each chunk.
#'   Cannot be used in combination with \code{n.chunks}.
#'   If \code{ids} cannot be chunked evenly, some chunks will have less elements.
#' @param n.chunks [\code{integer(1)}]\cr
#'   Requested number of chunks.
#'   If more chunks than elements in \code{ids} are requested, empty chunks are
#'   dropped. Cannot be used in combination with \code{chunks.size}.
#' @param group.by [\code{character(0)}]\cr
#'   If \code{ids} is a \code{\link{data.frame}} with additional columns (besides \dQuote{job.id}),
#'   then the chunking is done in subgroups defined by the columns \code{group.by}.
#'   Usually not needed for a regular \code{\link{Registry}} as the jobs are likely homogeneous.
#'   For an \code{\link{ExperimentRegistry}} on the other hand, you can use \code{group.by}
#'   to first partition the jobs into homogeneous groups and then chunk them (c.f. example).
#' @template reg
#' @return [\code{\link[data.table]{data.table}}]. Table \code{ids} with additional column
#'   \dQuote{job.id}.
#' @export
#' @examples
#' # chunking for Registry
#' reg = makeTempRegistry(make.default = FALSE)
#' ids = batchMap(identity, 1:25, reg = reg)
#' ids = chunkIds(ids, chunk.size = 10, reg = reg)
#' print(ids)
#' print(table(ids$chunk))
#'
#' # chunking for ExperimentRegistry
#' reg = makeTempExperimentRegistry(make.default = FALSE)
#' prob = addProblem(reg = reg, "prob1", data = iris, fun = function(data) nrow(data))
#' prob = addProblem(reg = reg, "prob2", data = Titanic, fun = function(data) nrow(data))
#' algo = addAlgorithm(reg = reg, "algo", fun = function(problem, i, ...) problem)
#' prob.designs = list(prob1 = data.table(), prob2 = data.table(x = 1:2))
#' algo.designs = list(algo = data.table(i = 1:5))
#' addExperiments(prob.designs, algo.designs, repls = 10, reg = reg)
#'
#' # -> group into chunks of 5 jobs, but do not mix problems
#' ids = getJobInfo(reg = reg)[, .(job.id, problem, algorithm)]
#' ids = chunkIds(ids, chunk.size = 5, group.by = "problem", reg = reg)
#' print(ids)
#' dcast(ids, chunk ~ problem)
chunkIds = function(ids = NULL, n.chunks = NULL, chunk.size = NULL, group.by = character(0L), reg = getDefaultRegistry()) {
  chunk = function(x, n.chunks, chunk.size) {
    n = length(x)
    if (n == 0L)
      return(integer(0L))
    if (is.null(n.chunks))
      n.chunks = (n %/% chunk.size + (n %% chunk.size > 0L))
    sample(as.integer((seq.int(0L, n - 1L) %% min(n.chunks, n))) + 1L)
  }

  assertRegistry(reg)
  ids = asIds(reg, ids, default = findJobs(reg = reg), extra.cols = TRUE)
  assertCharacter(group.by, any.missing = FALSE)

  x = is.null(n.chunks) + is.null(chunk.size)
  if (x == 0L) {
    stop("Either 'n.chunks' or 'chunk.size' must be set")
  } else if (x == 2L) {
    n.chunks = 1L
  } else {
    if (!is.null(n.chunks))
      n.chunks = asCount(n.chunks, positive = TRUE)
    if (!is.null(chunk.size))
      chunk.size = asCount(chunk.size, positive = TRUE)
  }

  if (length(group.by) > 0L) {
    if (!all(group.by %in% names(ids)))
      stop("All columns to group by must be provided in the 'ids' table")
    ids[, "chunk" := chunk(get("job.id"), n.chunks = n.chunks, chunk.size = chunk.size), by = group.by]
    ids[, "chunk" := .GRP, by = c(group.by, "chunk")]
  } else {
    ids[, "chunk" := chunk(get("job.id"), n.chunks = n.chunks, chunk.size = chunk.size)]
  }
  return(ids[])
}
