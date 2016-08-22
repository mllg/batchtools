#' @title Map Operation for Batch Systems
#'
#' @description
#' A parallel \code{\link[base]{Map}} for batch systems.
#' Note that this function only defines the jobs.
#' The actual computation is started with \code{\link{submitJobs}} and results
#' can be collected with \code{\link{reduceResultsList}}, \code{\link{reduceResults}} or
#' \code{\link{loadResult}}.
#' For a synchronous \code{\link[base]{Map}}-like execution see \code{\link{btmapply}}.
#'
#' @param fun [\code{function}]\cr
#'   Function to map over \code{...}.
#' @param ... [any]\cr
#'   Arguments to vectorize over (list or vector).
#'   Shorter vectors will be recycled (possibly with a warning any length is not a multiple of the maximum length).
#'   Mutually exclusive with \code{args}.
#' @param args [\code{list} | \code{data.frame}]\cr
#'   Arguments to vectorize over as (named) list or data frame.
#'   Shorter vectors will be recycled (possibly with a warning any length is not a multiple of the maximum length).
#'   Mutually exclusive with \code{...}.
#' @param more.args [\code{list}]\cr
#'   A list of other arguments passed to \code{fun}.
#'   Default is an empty list.
#' @template reg
#' @return [\code{\link{data.table}}]. Generated job ids are stored in the column \dQuote{job.id}.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @export
#' @examples
#' # example using "..." and more.args
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(x, y) x^2 + y
#' ids = batchMap(f, x = 1:10, more.args = list(y = 100), reg = reg)
#' getJobPars(reg = reg)
#' testJob(6, reg = reg) # 100 + 6^2 = 136
#'
#' # vector recycling
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(...) list(...)
#' ids = batchMap(f, x = 1:3, y = 1:6, reg = reg)
#' getJobPars(reg = reg)
#'
#' # example for an expand.grid()-like operation on parameters
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(paste, args = CJ(x = letters[1:3], y = 1:3), more.args = list(sep = ""), reg = reg)
#' getJobPars(reg = reg)
#' testJob(6, reg = reg)
batchMap = function(fun, ..., args = list(), more.args = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, strict = TRUE)
  if (nrow(reg$defs) > 0L)
    stop("Registry must be empty")
  assertFunction(fun)
  assert(checkList(args), checkDataFrame(args))
  assertList(more.args, names = "strict")

  if (length(args) > 0L) {
    if (length(list(...)) > 0L)
      stop("You may only provide arguments via '...' *or* 'args'")
    ddd = list2dt(args)
  } else {
    ddd = list2dt(list(...))
  }

  if (any(dim(ddd) == 0L))
    return(copy(noids))
  info("Adding %i jobs ...", nrow(ddd))

  writeRDS(fun, file = file.path(reg$file.dir, "user.function.rds"))
  if (length(more.args) > 0L)
    writeRDS(more.args, file = file.path(reg$file.dir, "more.args.rds"))
  ids = seq_row(ddd)

  reg$defs = data.table(
    def.id = ids,
    pars   = .mapply(list, dots = ddd, MoreArgs = list()),
    key    = "def.id")
  reg$defs$pars.hash = vcapply(reg$defs$pars, digest::digest)

  reg$status = data.table(
    job.id      = ids,
    def.id      = ids,
    submitted   = NA_integer_,
    started     = NA_integer_,
    done        = NA_integer_,
    error       = NA_character_,
    memory      = NA_real_,
    resource.id = NA_integer_,
    batch.id    = NA_character_,
    job.hash    = NA_character_,
    key         = "job.id")

  saveRegistry(reg)
  invisible(ids(reg$status))
}
