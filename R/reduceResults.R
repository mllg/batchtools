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
#'   is also passed to the function (named).
#' @param init [\code{ANY}]\cr
#'   Initial element, as used in \code{\link[base]{Reduce}}.
#'   If missing, the reduction uses the result of the first job as \code{init} and the reduction starts
#'   with the second job.
#' @param ... [\code{ANY}]\cr
#'   Additional arguments passed to function \code{fun}.
#' @return Aggregated results in the same order as provided ids.
#'   Return type depends on the user function. If \code{ids}
#'   is empty, \code{reduceResults} returns \code{init} (if available) or \code{NULL} otherwise.
#' @template reg
#' @family Results
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(a, b) list(sum = a+b, prod = a*b), a = 1:3, b = 1:3, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # Extract element sum from each result
#' reduceResults(function(aggr, res) c(aggr, res$sum), init = list(), reg = tmp)
#'
#' # Aggregate element sum via '+'
#' reduceResults(function(aggr, res) aggr + res$sum, init = 0, reg = tmp)
#'
#' # Aggregate element prod via '*' where parameter b < 3
#' reduce = function(aggr, res, job) {
#'   if (job$pars$b >= 3)
#'     return(aggr)
#'   aggr * res$prod
#' }
#' reduceResults(reduce, init = 1, reg = tmp)
#'
#' # Reduce to data.frame() (inefficient, use reduceResultsDataTable() instead)
#' reduceResults(rbind, init = data.frame(), reg = tmp)
#'
#' # Reduce to data.frame by collecting results first, then utilize vectorization of rbind:
#' res = reduceResultsList(fun = as.data.frame, reg = tmp)
#' do.call(rbind, res)
#'
#' # Reduce with custom combine function:
#' comb = function(x, y) list(sum = x$sum + y$sum, prod = x$prod * y$prod)
#' reduceResults(comb, reg = tmp)
#'
#' # The same with neutral element NULL
#' comb = function(x, y) if (is.null(x)) y else list(sum = x$sum + y$sum, prod = x$prod * y$prod)
#' reduceResults(comb, init = NULL, reg = tmp)
#'
#' # Alternative: Reduce in list, reduce manually in a 2nd step
#' res = reduceResultsList(reg = tmp)
#' Reduce(comb, res)
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
#' \dontshow{ batchtools:::example_push_temp(2) }
#' ### Example 1 - reduceResultsList
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) x^2, x = 1:10, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' reduceResultsList(fun = sqrt, reg = tmp)
#'
#' ### Example 2 - reduceResultsDataTable
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#'
#' # add first problem
#' fun = function(job, data, n, mean, sd, ...) rnorm(n, mean = mean, sd = sd)
#' addProblem("rnorm", fun = fun, reg = tmp)
#'
#' # add second problem
#' fun = function(job, data, n, lambda, ...) rexp(n, rate = lambda)
#' addProblem("rexp", fun = fun, reg = tmp)
#'
#' # add first algorithm
#' fun = function(instance, method, ...) if (method == "mean") mean(instance) else median(instance)
#' addAlgorithm("average", fun = fun, reg = tmp)
#'
#' # add second algorithm
#' fun = function(instance, ...) sd(instance)
#' addAlgorithm("deviation", fun = fun, reg = tmp)
#'
#' # define problem and algorithm designs
#' prob.designs = algo.designs = list()
#' prob.designs$rnorm = CJ(n = 100, mean = -1:1, sd = 1:5)
#' prob.designs$rexp = data.table(n = 100, lambda = 1:5)
#' algo.designs$average = data.table(method = c("mean", "median"))
#' algo.designs$deviation = data.table()
#'
#' # add experiments and submit
#' addExperiments(prob.designs, algo.designs, reg = tmp)
#' submitJobs(reg = tmp)
#'
#' # collect results and join them with problem and algorithm paramters
#' res = ijoin(
#'   getJobPars(reg = tmp),
#'   reduceResultsDataTable(reg = tmp, fun = function(x) list(res = x))
#' )
#' unwrap(res, sep = ".")
reduceResultsList = function(ids = NULL, fun = NULL, ..., missing.val, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertFunction(fun, null.ok = TRUE)
  ids = convertIds(reg, ids, default = .findDone(reg = reg), keep.order = TRUE)
  .reduceResultsList(ids, fun, ..., missing.val = missing.val, reg = reg)
}

#' @export
#' @rdname reduceResultsList
reduceResultsDataTable = function(ids = NULL, fun = NULL, ..., missing.val, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = convertIds(reg, ids, default = .findDone(reg = reg))
  assertFunction(fun, null.ok = TRUE)

  results = .reduceResultsList(ids = ids, fun = fun, ..., missing.val = missing.val, reg = reg)
  if (length(results) == 0L)
    return(noIds())
  ids[, "result" := results][]
}

.reduceResultsList = function(ids, fun = NULL, ..., missing.val, reg = getDefaultRegistry()) {
  if (is.null(fun)) {
    worker = function(.res, .job, ...) .res
  } else {
    fun = match.fun(fun)
    if ("job" %chin% names(formals(fun)))
      worker = function(.res, .job, ...) fun(.res, job = .job, ...)
    else
      worker = function(.res, .job, ...) fun(.res, ...)
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
    reader = RDSReader$new(TRUE)

    for (i in done) {
      res = worker(readRDS(fns[i]), makeJob(ids$job.id[i], reader = reader, reg = reg), ...)
      if (!is.null(res))
        results[[i]] = res
      rm(res)
      pb$tick()
    }
  }
  return(results)
}
