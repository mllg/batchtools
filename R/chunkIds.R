#' @title Chunk Jobs for Sequential Execution
#'
#' @description
#' Partition jobs into \dQuote{chunks} which will be executed together on the nodes.
#'
#' Chunks are submitted via \code{\link{submitJobs}} by simply providing
#' a data frame with columns \dQuote{job.id} and corresponding \dQuote{chunk} (both integer).
#' All jobs with the same chunk number will be grouped together on one node as a single
#' computational job.
#'
#' If neither \code{n.chunks} nor \code{chunk.size} are provided, each job will be assigned
#' to its own chunk.
#'
#' @templateVar ids.default all
#' @template ids
#' @param chunk.size [\code{integer(1)}]\cr
#'   Requested number of elements in each chunk.
#'   If \code{ids} cannot be chunked evenly, some chunks will have less elements than others.
#'   Mutually exclusive with with \code{n.chunks}.
#' @param n.chunks [\code{integer(1)}]\cr
#'   Requested number of chunks.
#'   If more chunks than elements in \code{ids} are requested, empty chunks are ignored.
#'   Mutually exclusive with \code{chunks.size}.
#' @param group.by [\code{character(0)}]\cr
#'   If \code{ids} is a \code{\link{data.frame}} with additional columns
#'   (in addition to the required column \dQuote{job.id}), then the chunking is performed using
#'   subgroups defined by the columns set in \code{group.by}.
#'   See example.
#' @template reg
#' @return [\code{\link[data.table]{data.table}}] with columns \dQuote{job.id} and \dQuote{chunk}.
#' @export
#' @examples
#' # chunking for Registry
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:25, reg = tmp)
#' ids = chunkIds(chunk.size = 10, reg = tmp)
#' print(ids)
#' print(table(ids$chunk))
#'
#' # Creating chunks for an ExperimentRegistry
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#' prob = addProblem(reg = tmp, "prob1", data = iris, fun = function(job, data) nrow(data))
#' prob = addProblem(reg = tmp, "prob2", data = Titanic, fun = function(job, data) nrow(data))
#' algo = addAlgorithm(reg = tmp, "algo", fun = function(job, data, instance, i, ...) problem)
#' prob.designs = list(prob1 = data.table(), prob2 = data.table(x = 1:2))
#' algo.designs = list(algo = data.table(i = 1:3))
#' addExperiments(prob.designs, algo.designs, repls = 3, reg = tmp)
#'
#' # group into chunks of 5 jobs, but do not put multiple problems into a single chunk
#' # -> only one problem has to be loaded per chunk, and only once because it is then cached.
#' ids = getJobTable(reg = tmp)[, .(job.id, problem, algorithm)]
#' chunked = chunkIds(ids, chunk.size = 5, group.by = "problem", reg = tmp)
#' print(chunked)
#' dcast(ijoin(ids, chunked), chunk ~ problem)
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
  assertCharacter(group.by, any.missing = FALSE, min.chars = 1L)

  ids = convertIds(reg, ids, default = allIds(reg), keep.extra = group.by)
  job.id = NULL

  x = is.null(n.chunks) + is.null(chunk.size)
  if (x == 0L) {
    stop("Either 'n.chunks' or 'chunk.size' must be set")
  } else if (x == 2L) {
    n.chunks = nrow(ids)
  } else {
    if (!is.null(n.chunks))
      n.chunks = asCount(n.chunks, positive = TRUE)
    if (!is.null(chunk.size))
      chunk.size = asCount(chunk.size, positive = TRUE)
  }

  if (length(group.by) > 0L) {
    if (any(group.by %nin% names(ids)))
      stop("All columns to group by must be provided in the 'ids' table")
    ids[, "chunk" := chunk(job.id, n.chunks = n.chunks, chunk.size = chunk.size), by = group.by]
    ids[, "chunk" := .GRP, by = c(group.by, "chunk")]
  } else {
    ids[, "chunk" := chunk(job.id, n.chunks = n.chunks, chunk.size = chunk.size)]
  }

  return(ids[, c("job.id", "chunk"), with = FALSE])
}
