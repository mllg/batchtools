#' @title Map Over Results to Create New Jobs
#'
#' @description
#' This function allows you to create new computational jobs (just like \code{\link{batchMap}} based on the results of
#' a \code{\link{Registry}}.
#'
#' @templateVar ids.default findDone
#' @param fun [\code{function}]\cr
#'   Function which takes the result as first (unnamed) argument.
#' @template ids
#' @param ... [any]\cr
#'   Arguments to vectorize over (list or vector). Passed to \code{\link{batchMap}}<`3`>.
#' @template missing.val
#' @template more.args
#' @param target [\code{\link{Registry}}]\cr
#'   Empty Registry where new jobs are created for.
#' @param source [\code{\link{Registry}}]\cr
#'   Registry. If not explicitly passed, uses the default registry (see \code{\link{setDefaultRegistry}}).
#' @return [\code{\link{data.table}}] with ids of jobs added to \code{target}.
#' @export
#' @family Results
#' @examples
#' # source registry: calculate squre of some numbers
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) list(square = x^2), x = 1:10, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # target registry: map results of first registry and calculate the square root
#' target = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMapResults(fun = function(x, y) list(sqrt = sqrt(x$square)), target = target, source = tmp)
#' getStatus(reg = target)
#' submitJobs(reg = target)
#' waitForJobs(reg = target)
#'
#' rjoin(reduceResultsDataTable(reg = tmp), reduceResultsDataTable(reg = target))
batchMapResults = function(fun, ids = NULL, ..., missing.val, more.args = list(), target, source = getDefaultRegistry()) {
  assertRegistry(source, sync = TRUE)
  assertRegistry(target, sync = TRUE)
  assertFunction(fun)
  ids = convertIds(source, ids, default = .findDone(reg = source))
  assertList(more.args, names = "strict")

  if (nrow(target$status) > 0L)
    stop("Target registry 'target' must be empty")

  more.args = c(list(..file.dir = source$file.dir, ..ids = ids, ..fun = fun), more.args)
  if (!missing(missing.val))
    more.args["..missing.val"] = list(missing.val)
  args = c(list(..i = seq_row(ids)), list(...))

  batchMap(batchMapResultsWrapper, args = args, more.args = more.args, reg = target)
}

batchMapResultsWrapper = function(..file.dir, ..fun, ..ids, ..i, ..missing.val, ...) {
  ..fun(.loadResult(..file.dir, ..ids[..i], ..missing.val), ...)
}
