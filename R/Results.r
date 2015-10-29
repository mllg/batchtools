#' Reduce Results
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   A function to reduce the results. The result of previous iterations (or
#'   the \code{init}) will be passed as first argument, the result of of the i-th
#'   iteration as second. See \code{\link[base]{Reduce}} for some examples.
#'   If the function has the formal argument \dQuote{job}, the job description is
#'   passed.
#' @param init [\code{ANY}]\cr
#'   Initial element, as used in \code{\link[base]{Reduce}}.
#'   Default is the first result.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @return Aggregated results, return type depends on function. If \code{ids} is empty, \code{reduceResults}
#'   returns \code{init} (if available) or \code{NULL} otherwise.
#' @template reg
#' @family Results
#' @export
reduceResults = function(fun, ids = NULL, init, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findDone(reg = reg))
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
      init = forceAndCall(3L, fun, init, readRDS(fns[i]), job = makeJobDescription(ids[i], reg = reg), ...)
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

#' @title Aggregate Results
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   Function to apply to each result. The results are passed unnamed as first
#'   argument.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @template reg
#' @return \code{reduceResultsList} returns a list, \code{reduceResultsDataTable}
#'   returns a \code{\link[data.table]{data.table}} with
#'   columns \dQuote{job.id} and \dQuote{result}.
#' @seealso \code{\link{reduceResults}}.
#' @family Results
#' @export
reduceResultsList = function(ids = NULL, fun = NULL, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findDone(reg = reg))

  if (is.null(fun)) {
    getResult = function(fn, ...) readRDS(file = fn)
  } else {
    fun = match.fun(fun)
    getResult = function(fn, ...) fun(readRDS(fn), ...)
  }

  fns = file.path(reg$file.dir, "results", sprintf("%i.rds", ids$job.id))
  n = length(fns)
  if (n == 0L)
    return(list())

  results = vector("list", n)
  pb = makeProgressBar(total = n, format = "Submit [:bar] :percent eta: :eta")
  for (i in which(file.exists(fns))) {
    results[[i]] = getResult(fns[i], ...)
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
  ids = asIds(reg, ids, default = .findDone(reg = reg))
  assertFlag(fill)
  results = reduceResultsList(ids = ids, fun = fun, ..., reg = reg)
  if (!all(vlapply(results, is.list)))
    results = lapply(results, as.list)
  cbind(ids, rbindlist(results, fill = fill))
}

#' @title Load the Result of a Single Job
#'
#' @template id
#' @param missing.val [\code{ANY}]\cr
#'   Value to return if the result file is missing. Default is \code{NULL}.
#' @template reg
#' @return [\code{ANY}]. The saved result or \code{missing.val} if result file is not found.
#' @family Results
#' @export
loadResult = function(id, missing.val = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  id = asIds(reg, id, n = 1L)
  fn = file.path(reg$file.dir, "results", sprintf("%i.rds", id$job.id))
  if (!file.exists(fn))
    return(missing.val)
  return(readRDS(fn))
}
