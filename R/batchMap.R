#' @title Map Operation for Batch Systems
#'
#' @description
#' A parallel and asynchronous \code{\link[base]{Map}}/\code{\link[base]{mapply}} for batch systems.
#' Note that this function only defines the computational jobs.
#' The actual computation is started with \code{\link{submitJobs}}.
#' Results and partial results can be collected with \code{\link{reduceResultsList}}, \code{\link{reduceResults}} or
#' \code{\link{loadResult}}.
#'
#' For a synchronous \code{\link[base]{Map}}-like execution, see \code{\link{btmapply}}.
#'
#' @param fun [\code{function}]\cr
#'   Function to map over arguments provided via \code{...}.
#'   Parameters given via \code{args} or \code{...} are passed as-is, in the respective order and possibly named.
#'   If the function has the named formal argument \dQuote{.job}, the \code{\link{Job}} is passed to the function
#'   on the slave.
#' @param ... [ANY]\cr
#'   Arguments to vectorize over (list or vector).
#'   Shorter vectors will be recycled (possibly with a warning any length is not a multiple of the longest length).
#'   Mutually exclusive with \code{args}.
#'   Note that although it is possible to iterate over large objects (e.g., lists of data frames or matrices), this usually
#'   hurts the overall performance and thus is discouraged.
#' @param args [\code{list} | \code{data.frame}]\cr
#'   Arguments to vectorize over as (named) list or data frame.
#'   Shorter vectors will be recycled (possibly with a warning any length is not a multiple of the longest length).
#'   Mutually exclusive with \code{...}.
#' @template more.args
#' @template reg
#' @return [\code{\link{data.table}}] with ids of added jobs stored in column \dQuote{job.id}.
#' @export
#' @seealso \code{\link{batchReduce}}
#' @examples
#' \dontshow{ batchtools:::example_push_temp(3) }
#' # example using "..." and more.args
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(x, y) x^2 + y
#' ids = batchMap(f, x = 1:10, more.args = list(y = 100), reg = tmp)
#' getJobPars(reg = tmp)
#' testJob(6, reg = tmp) # 100 + 6^2 = 136
#'
#' # vector recycling
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(...) list(...)
#' ids = batchMap(f, x = 1:3, y = 1:6, reg = tmp)
#' getJobPars(reg = tmp)
#'
#' # example for an expand.grid()-like operation on parameters
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(paste, args = data.table::CJ(x = letters[1:3], y = 1:3), reg = tmp)
#' getJobPars(reg = tmp)
#' testJob(6, reg = tmp)
batchMap = function(fun, ..., args = list(), more.args = list(), reg = getDefaultRegistry()) {
  list2dt = function(x) { # converts a list to a data.table, but avoids creating column names
    nn = names(x)
    if (is.null(nn))
      names(x) = rep.int("", length(x))
    as.data.table(x)
  }

  assertRegistry(reg, class = "Registry", writeable = TRUE)
  if (nrow(reg$defs) > 0L)
    stop("Registry must be empty")
  assertFunction(fun)
  assert(checkList(args), checkDataFrame(args))
  assertList(more.args)

  if (length(args) > 0L) {
    if (...length() > 0L)
      stop("You may only provide arguments via '...' *or* 'args'")
    ddd = list2dt(args)
  } else {
    ddd = list2dt(list(...))
  }

  if (".job" %chin% names(ddd))
    stop("Name '.job' not allowed as parameter name (reserved keyword)")

  if (any(dim(ddd) == 0L))
    return(noIds())
  info("Adding %i jobs ...", nrow(ddd))

  writeRDS(fun, file = fs::path(reg$file.dir, "user.function.rds"), compress = reg$compress)
  if (length(more.args) > 0L)
    writeRDS(more.args, file = fs::path(reg$file.dir, "more.args.rds"), compress = reg$compress)
  ids = seq_row(ddd)

  reg$defs = data.table(
    def.id   = ids,
    job.pars = .mapply(list, dots = ddd, MoreArgs = list()),
    key      = "def.id")

  reg$status = data.table(
    job.id      = ids,
    def.id      = ids,
    submitted   = NA_real_,
    started     = NA_real_,
    done        = NA_real_,
    error       = NA_character_,
    mem.used    = NA_real_,
    resource.id = NA_integer_,
    batch.id    = NA_character_,
    log.file    = NA_character_,
    job.hash    = NA_character_,
    job.name    = NA_character_,
    key         = "job.id")

  saveRegistry(reg)
  invisible(allIds(reg))
}
