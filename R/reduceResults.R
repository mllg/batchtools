#' @title Reduce Results
#'
#' @description
#' A version of \code{\link[base]{Reduce}} for \code{\link{Registry}} objects
#' which iterates over finished jobs and aggregates them.
#' All jobs must have terminated, an error is raised otherwise.
#'
#' @note
#' If you have thousands of jobs, disabling the progress bar (\code{options(batchtools.progress = FALSE)})
#' can significantly increase the performance.
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   A function to reduce the results. The result of previous iterations (or
#'   the \code{init}) will be passed as first argument, the result of of the
#'   i-th iteration as second. See \code{\link[base]{Reduce}} for some
#'   examples.
#'   If the function has the formal argument \dQuote{job}, the \code{\link{Job}}/\code{\link{Experiment}}
#'   is also passed to the function.
#' @param init [\code{ANY}]\cr
#'   Initial element, as used in \code{\link[base]{Reduce}}.
#'   Default is the first result.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @return Aggregated results in the same order as provided ids.
#'   Return type depends on the user function. If \code{ids}
#'   is empty, \code{reduceResults} returns \code{init} (if available) or \code{NULL} otherwise.
#' @template reg
#' @family Results
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) x^2, x = 1:10, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' reduceResults(function(x, y) c(x, y), reg = tmp)
#' reduceResults(function(x, y) c(x, sqrt(y)), init = numeric(0), reg = tmp)
reduceResults = function(fun, ids = NULL, init, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = convertIds(reg, ids, default = .findDone(reg = reg), keep.order = TRUE)
  fun = match.fun(fun)
  if (nrow(.findNotDone(reg, ids)))
    stop("All jobs must be have been successfully computed")

  if (nrow(ids) == 0L)
    return(if (missing(init)) NULL else init)

  fns = getResultFiles(reg, ids)
  if (missing(init)) {
    init = readRDS(fns[1L])
    fns = fns[-1L]
    if (length(fns) == 0L)
      return(init)
  }

  pb = makeProgressBar(total = length(fns), format = "Reduce [:bar] :percent eta: :eta")
  if ("job" %chin% names(formals(fun))) {
    for (i in seq_along(fns)) {
      init = fun(init, readRDS(fns[i]), job = makeJob(ids[i], reg = reg), ...)
      pb$tick()
    }
  } else {
    for (i in seq_along(fns)) {
      init = fun(init, readRDS(fns[i]), ...)
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
#' The later requires the provided function to return a list (or \code{data.frame}) of scalar values.
#' See \code{\link[data.table]{rbindlist}} for features and limitations of the aggregation.
#'
#' If not all jobs are terminated, the respective result will be \code{NULL}.
#'
#' @note
#' If you have thousands of jobs, disabling the progress bar (\code{options(batchtools.progress = FALSE)})
#' can significantly increase the performance.
#'
#' @templateVar ids.default findDone
#' @template ids
#' @param fun [\code{function}]\cr
#'   Function to apply to each result. The result is passed unnamed as first argument. If \code{NULL}, the identity is used.
#'   If the function has the formal argument \dQuote{job}, the \code{\link{Job}}/\code{\link{Experiment}} is also passed to the function.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to to function \code{fun}.
#' @template missing.val
#' @template reg
#' @return \code{reduceResultsList} returns a list of the results in the same order as the provided ids.
#'   \code{reduceResultsDataTable} returns a \code{\link[data.table]{data.table}} with columns \dQuote{job.id} and additional result columns
#'   created via \code{\link[data.table]{rbindlist}}, sorted by \dQuote{job.id}.
#' @seealso \code{\link{reduceResults}}
#' @family Results
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) x^2, x = 1:10, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' reduceResultsList(fun = sqrt, reg = tmp)
reduceResultsList = function(ids = NULL, fun = NULL, ..., missing.val, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertFunction(fun, null.ok = TRUE)
  ids = convertIds(reg, ids, default = .findDone(reg = reg), keep.order = TRUE)
  .reduceResultsList(ids, fun, ..., missing.val = missing.val, reg = reg)
}

#' @param fill [\code{logical(1)}]\cr
#'   In \code{reduceResultsDataTable}: This flag is passed down to
#'   \code{\link[data.table]{rbindlist}} which is used to convert the results
#'   to a \code{\link[data.table]{data.table}}.
#' @export
#' @rdname reduceResultsList
reduceResultsDataTable = function(ids = NULL, fun = NULL, ..., fill = FALSE, missing.val, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = convertIds(reg, ids, default = .findDone(reg = reg))
  assertFunction(fun, null.ok = TRUE)
  assertFlag(fill)

  results = .reduceResultsList(ids = ids, fun = fun, ..., missing.val = missing.val, reg = reg)
  if (length(results) == 0L)
    return(noIds())
  if (!qtestr(results, "d"))
    results = lapply(results, as.data.table)
  results = rbindlist(results, fill = fill, idcol = "job.id")
  if (!identical(results$job.id, seq_row(ids)))
    stop("The function must return an object for each job which is convertible to a data.frame with one row")
  results[, "job.id" := ids$job.id]
  setkeyv(results, "job.id")[]
}

.reduceResultsList = function(ids, fun = NULL, ..., missing.val, reg = getDefaultRegistry()) {
  if (is.null(fun)) {
    worker = function(..res, ..job, ...) ..res
  } else {
    fun = match.fun(fun)
    if ("job" %chin% names(formals(fun)))
      worker = function(..res, ..job, ...) fun(..res, job = ..job, ...)
    else
      worker = function(..res, ..job, ...) fun(..res, ...)
  }

  results = vector("list", nrow(ids))
  done = ids[.findDone(reg, ids), nomatch = 0L, which = TRUE, on = "job.id"]

  if (missing(missing.val)) {
    if (length(done) != nrow(ids))
      stop("All jobs must be have been successfully computed")
  } else {
    results[setdiff(seq_row(ids), done)] = list(missing.val)
  }

  if (length(done) > 0L) {
    fns = getResultFiles(reg, ids)
    pb = makeProgressBar(total = length(fns), format = "Reducing [:bar] :percent eta: :eta")
    cache = Cache$new(reg$file.dir)

    for (i in done) {
      res = worker(readRDS(fns[i]), makeJob(ids$job.id[i], cache = cache, reg = reg), ...)
      if (!is.null(res))
        results[[i]] = res
      pb$tick()
    }
  }
  return(results)
}
