#' @title Reduce Results
#'
#' @description
#' A version of \code{\link[base]{Reduce}} for \code{\link{Registry}} objects
#' which iterates over finished jobs and aggregates them.
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   A function to reduce the results. The result of previous iterations (or
#'   the \code{init}) will be passed as first argument, the result of of the
#'   i-th iteration as second. See \code{\link[base]{Reduce}} for some
#'   examples.
#'   If the function has the formal argument \dQuote{job}, the \code{\link{Job}}/\code{\link{Experiment}}
#'   is passed to the function.
#' @param init [\code{ANY}]\cr
#'   Initial element, as used in \code{\link[base]{Reduce}}.
#'   Default is the first result.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @return Aggregated results, return type depends on function. If \code{ids}
#'   is empty, \code{reduceResults} returns \code{init} (if available) or
#'   \code{NULL} otherwise.
#' @template reg
#' @family Results
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) x^2, x = 1:10, reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#' reduceResults(function(x, y) c(x, y), reg = reg)
#' reduceResults(function(x, y) c(x, sqrt(y)), init = numeric(0), reg = reg)
reduceResults = function(fun, ids = NULL, init, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = asJobTable(reg, ids, default = .findDone(reg))
  fun = match.fun(fun)

  fns = sprintf("%i.rds", ids$job.id)
  if (length(fns) == 0L)
    return(if (missing(init)) NULL else init)

  fns = file.path(reg$file.dir, "results", fns)
  if (missing(init)) {
    init = readRDS(fns[1L])
    fns = fns[-1L]
    if (length(fns) == 0L)
      return(init)
  }

  pb = makeProgressBar(total = length(fns), format = "Reduce [:bar] :percent eta: :eta")
  if ("job" %in% names(formals(fun))) {
    for (i in seq_along(fns)) {
      init = forceAndCall(3L, fun, init, readRDS(fns[i]), job = makeJob(ids[i], reg = reg), ...)
      pb$tick()
    }
  } else {
    for (i in seq_along(fns)) {
      init = forceAndCall(2L, fun, init, readRDS(fns[i]), ...)
      pb$tick()
    }
  }
  return(init)
}

#' @title Apply Functions on Results
#'
#' @description
#' Applies a function on the results of your finished jobs and thereby collects
#' them in a \code{\link[base]{list}} or \code{\link[data.table]{data.table}}.
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   Function to apply to each result. The result is passed unnamed as first
#'   argument. If \code{NULL}, the identity is used.
#'   If the function has the formal argument \dQuote{job}, the \code{\link{Job}}/\code{\link{Experiment}}
#'   is passed to the function.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @template reg
#' @return \code{reduceResultsList} returns a list, \code{reduceResultsDataTable}
#'   returns a \code{\link[data.table]{data.table}} with
#'   columns \dQuote{job.id} and additional result columns as returned by (see \code{\link[data.table]{rbindlist}}).
#' @seealso \code{\link{reduceResults}}.
#' @family Results
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) x^2, x = 1:10, reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#' reduceResultsList(fun = sqrt, reg = reg)
reduceResultsList = function(ids = NULL, fun = NULL, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = asJobTable(reg, ids, default = .findDone(reg = reg))

  fns = file.path(reg$file.dir, "results", sprintf("%i.rds", ids$job.id))
  n = length(fns)
  if (n == 0L)
    return(list())

  if (is.null(fun)) {
    worker = function(..res, ..job, ...) ..res
  } else {
    fun = match.fun(fun)
    if ("job" %in% names(formals(fun)))
      worker = function(..res, ..job, ...) fun(..res, job = ..job, ...)
    else
      worker = function(..res, ..job, ...) fun(..res, ...)
  }

  results = vector("list", n)
  pb = makeProgressBar(total = n, format = "Reducing [:bar] :percent eta: :eta")
  cache = Cache$new(reg$file.dir)
  for (i in which(file.exists(fns))) {
    results[[i]] = worker(readRDS(fns[i]), makeJob(ids$job.id[i], cache = cache, reg = reg), ...)
    pb$tick()
  }
  return(results)
}

#' @param fill [\code{logical(1)}]\cr
#'   In \code{reduceResultsDataTable}: This flag is passed down to
#'   \code{\link[data.table]{rbindlist}} which is used to convert the results
#'   to a \code{\link[data.table]{data.table}}.
#' @export
#' @rdname reduceResultsList
reduceResultsDataTable = function(ids = NULL, fun = NULL, ..., fill = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = asJobTable(reg, ids, default = .findDone(reg = reg))
  assertFlag(fill)
  results = reduceResultsList(ids = ids, fun = fun, ..., reg = reg)
  if (!all(vlapply(results, is.data.table)))
    results = lapply(results, as.data.table)
  results = rbindlist(results, fill = fill, idcol = "..id")
  if (!identical(results$..id, seq_row(ids)))
    stop("The function must return an object for each job which is convertible to a data.frame with one row")
  results$..id = NULL
  cbind(ids, results)
}

#' @title Load the Result of a Single Job
#'
#' @description
#' A function to simply load the results for a single job.
#'
#' @template id
#' @param missing.val [\code{ANY}]\cr
#'   Value to return if the result file is missing. Default is \code{NULL}.
#' @template reg
#' @return [\code{ANY}]. The saved result or \code{missing.val} if result file
#'   is not found.
#' @family Results
#' @export
loadResult = function(id, missing.val = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  id = asJobTable(reg, id, single.id = TRUE)
  fn = file.path(reg$file.dir, "results", sprintf("%i.rds", id$job.id))
  if (!file.exists(fn))
    return(missing.val)
  return(readRDS(fn))
}
