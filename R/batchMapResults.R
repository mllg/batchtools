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
#' @param ... [ANY]\cr
#'   Arguments to vectorize over (list or vector). Passed to \code{\link{batchMap}}.
#' @template more.args
#' @param target [\code{\link{Registry}}]\cr
#'   Empty Registry where new jobs are created for.
#' @param source [\code{\link{Registry}}]\cr
#'   Registry. If not explicitly passed, uses the default registry (see \code{\link{setDefaultRegistry}}).
#' @return [\code{\link{data.table}}] with ids of jobs added to \code{target}.
#' @export
#' @family Results
#' @examples
#' # Source registry: calculate squre of some numbers
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) list(square = x^2), x = 1:10, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # Target registry: map some results of first registry to calculate the square root
#' target = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMapResults(fun = function(x, y) list(sqrt = sqrt(x$square)), ids = 4:8,
#'   target = target, source = tmp)
#' submitJobs(reg = target)
#' waitForJobs(reg = target)
#'
#' # Map old to new ids. First, get a table with results and parameters
#' results = unwrap(rjoin(getJobPars(reg = target), reduceResultsDataTable(reg = target)))
#' print(results)
#'
#' # Parameter '..id' points to job.id in 'source'. Use a inner join to combine:
#' ijoin(results, unwrap(reduceResultsDataTable(reg = tmp)), by = c("..id" = "job.id"))
batchMapResults = function(fun, ids = NULL, ..., more.args = list(), target, source = getDefaultRegistry()) {
  assertRegistry(source, sync = TRUE)
  assertRegistry(target, writeable = TRUE, sync = TRUE)
  assertFunction(fun)
  ids = convertIds(source, ids, default = .findDone(reg = source))
  assertList(more.args, names = "strict")

  if (nrow(target$status) > 0L)
    stop("Target registry 'target' must be empty")

  fns = getResultFiles(source, ids)
  names(fns) = ids$job.id
  more.args = c(list(..fn = fns, ..fun = fun), more.args)
  args = c(list(..id = ids$job.id), list(...))

  batchMap(batchMapResultsWrapper, args = args, more.args = more.args, reg = target)
}

batchMapResultsWrapper = function(..fun, ..fn, ..id, ...) {
  ..fun(readRDS(..fn[[as.character(..id)]]), ...)
}
