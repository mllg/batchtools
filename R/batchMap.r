#' @title Map Operation for Batch Systems
#'
#' @description
#' A parallel \code{\link[base]{Map}} for batch systems.
#'
#' @param fun [\code{function}]\cr
#'   Function to map over \code{...}.
#' @param ... [any]\cr
#'   Arguments to vectorize over (list or vector).
#' @param more.args [\code{list}]\cr
#'   A list of other arguments passed to \code{fun}.
#'   Default is an empty list.
#' @template reg
#' @return [\code{data.table}]. Generated job ids are stored in the column \dQuote{job.id}.
#' @export
#' @examples
#' reg = makeTempRegistry()
#' f = function(x, y) x^2 + y
#' ids = batchMap(f, x = 1:10, more.args = list(y = 100), reg = reg)
#' print(ids)
#' getJobInfo(reg = reg, pars.as.cols = TRUE)
batchMap = function(fun, ..., more.args = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, strict = TRUE)
  if (nrow(reg$defs) > 0L)
    stop("Registry must be empty")
  assertFunction(fun)
  assertList(more.args, names = "strict")

  ddd = list(...)
  if (length(ddd) == 0L)
    return(data.table(job.id = integer(0L), key = "job.id"))

  n = unique(lengths(ddd))
  if(length(n) != 1L) {
    mn = max(n)
    if (any(mn %% n != 0L))
      warning("longer argument not a multiple of length of shorter")
    ddd = lapply(ddd, rep_len, length.out = mn)
    n = mn
  }
  if (n == 0L)
    return(data.table(job.id = integer(0L), key = "job.id"))

  info("Adding %i jobs ...", n)

  writeRDS(fun, file = file.path(reg$file.dir, "user.function.rds"))
  if (length(more.args) > 0L)
    writeRDS(more.args, file = file.path(reg$file.dir, "more.args.rds"))
  ids = seq_len(n)

  reg$defs = data.table(
    def.id = ids,
    pars   = .mapply(list, dots = ddd, MoreArgs = list()),
    key = "def.id")
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
    key = "job.id")

  saveRegistry(reg)
  invisible(reg$status[, "job.id", with = FALSE])
}
