#' @title Estimate Remaining Runtimes
#'
#' @description
#' Estimates the runtimes of jobs using the random forest implemented in \pkg{ranger}.
#' Observed runtimes are retrieved from the \code{\link{Registry}} and runtimes are
#' predicted for unfinished jobs.
#'
#' The estimated remaining time is calculated in the \code{print} method.
#' You may also pass \code{n} here to determine the number of parallel jobs which is then used
#' in a simple Longest Processing Time (LPT) algorithm to give an estimate for the parallel runtime.
#'
#' @param tab [\code{\link{data.table}}]\cr
#'   Table with column \dQuote{job.id} and additional columns to predict the runtime.
#'   Observed runtimes will be looked up in the registry and serve as dependent variable.
#'   All columns in \code{tab} except \dQuote{job.id} will be passed to \code{\link[ranger]{ranger}} as
#'   independent variables to fit the model.
#' @param ... [ANY]\cr
#'   Additional parameters passed to \code{\link[ranger]{ranger}}. Ignored for the \code{print} method.
#' @template reg
#' @return [\code{RuntimeEstimate}] which is a \code{list} with two named elements:
#'  \dQuote{runtimes} is a \code{\link{data.table}} with columns \dQuote{job.id},
#'  \dQuote{runtime} (in seconds) and \dQuote{type} (\dQuote{estimated} if runtime is estimated,
#'  \dQuote{observed} if runtime was observed).
#'  The other element of the list named \dQuote{model}] contains the fitted random forest object.
#' @export
#' @seealso \code{\link{binpack}} and \code{\link{lpt}} to chunk jobs according to their estimated runtimes.
#' @examples
#' # Create a simple toy registry
#' set.seed(1)
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE, seed = 1)
#' addProblem(name = "iris", data = iris, fun = function(data, ...) nrow(data), reg = tmp)
#' addAlgorithm(name = "nrow", function(instance, ...) nrow(instance), reg = tmp)
#' addAlgorithm(name = "ncol", function(instance, ...) ncol(instance), reg = tmp)
#' addExperiments(algo.designs = list(nrow = CJ(x = 1:50, y = letters[1:5])), reg = tmp)
#' addExperiments(algo.designs = list(ncol = CJ(x = 1:50, y = letters[1:5])), reg = tmp)
#'
#' # We use the job parameters to predict runtimes
#' tab = getJobPars(reg = tmp)
#'
#' # First we need to submit some jobs so that the forest can train on some data.
#' # Thus, we just sample some jobs from the registry while grouping by factor variables.
#' ids = tab[, .SD[sample(nrow(.SD), 5)], by = c("problem", "algorithm", "y")]
#' setkeyv(ids, "job.id")
#' submitJobs(ids, reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # We "simulate" some more realistic runtimes here to demonstrate the functionality:
#' # - Algorithm "ncol" is 5 times more expensive than "nrow"
#' # - x has no effect on the runtime
#' # - If y is "a" or "b", the runtimes are really high
#' runtime = function(algorithm, x, y) {
#'   ifelse(algorithm == "nrow", 100L, 500L) + 1000L * (y %in% letters[1:2])
#' }
#' tmp$status[ids, done := done + tab[ids, runtime(algorithm, x, y)]]
#' rjoin(sjoin(tab, ids), getJobStatus(ids, reg = tmp)[, c("job.id", "time.running")])
#'
#' # Estimate runtimes:
#' est = estimateRuntimes(tab, reg = tmp)
#' print(est)
#' rjoin(tab, est$runtimes)
#' print(est, n = 10)
#'
#' # Submit jobs with longest runtime first:
#' ids = est$runtimes[type == "estimated"][order(runtime, decreasing = TRUE)]
#' print(ids)
#' \dontrun{
#' submitJobs(ids, reg = tmp)
#' }
#'
#' # Group jobs into chunks with runtime < 1h
#' ids = est$runtimes[type == "estimated"]
#' ids[, chunk := binpack(runtime, 3600)]
#' print(ids)
#' print(ids[, list(runtime = sum(runtime)), by = chunk])
#' \dontrun{
#' submitJobs(ids, reg = tmp)
#' }
#'
#' # Group jobs into 10 chunks with similar runtime
#' ids = est$runtimes[type == "estimated"]
#' ids[, chunk := lpt(runtime, 10)]
#' print(ids[, list(runtime = sum(runtime)), by = chunk])
estimateRuntimes = function(tab, ..., reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  data = copy(convertIds(reg, tab, keep.extra = names(tab)))

  if (!requireNamespace("ranger", quietly = TRUE))
    stop("Please install package 'ranger' for runtime estimation")

  data[, "runtime" := as.numeric(getJobStatus(tab, reg)$time.running)]
  i = is.na(data$runtime)
  if (all(i))
    stop("No training data available. Some jobs must be finished before estimating runtimes.")

  rf = ranger::ranger(runtime ~ ., data = data[!i, !"job.id"], ...)
  data[i, "runtime" := predict(rf, .SD)$predictions, .SDcols = chsetdiff(names(data), c("job.id", "runtime"))]
  data$type = factor(ifelse(i, "estimated", "observed"), levels = c("observed", "estimated"))
  setClasses(list(runtimes = data[, c("job.id", "type", "runtime")], model = rf), c("RuntimeEstimate", class(data)))
}


#' @rdname estimateRuntimes
#' @param x [\code{RuntimeEstimate}]\cr
#'   Object to print.
#' @param n [\code{integer(1)}]\cr
#'   Number of parallel jobs to assume for runtime estimation.
#' @export
print.RuntimeEstimate = function(x, n = 1L, ...) {
  ps = function(x, nc = 2L) {
    sprintf(paste0("%0", nc, "id %02ih %02im %.1fs"),
      floor(x / 86400),
      floor((x / 3600) %% 24L),
      floor((x / 60) %% 60L),
      x %% 60L
      )
  }

  assertCount(n, positive = TRUE)
  runtime = type = NULL
  calculated = x$runtimes[type == "observed", sum(runtime)]
  remaining = x$runtimes[type == "estimated", sum(runtime)]
  total = calculated + remaining
  nc = max(1L, nchar(total %/% 86400))

  catf("Runtime Estimate for %i jobs with %i CPUs", nrow(x$runtimes), n)
  catf("  Done     : %s", ps(calculated, nc = nc))
  catf("  Remaining: %s", ps(remaining, nc = nc))
  if (n >= 2L) {
    rt = x$runtimes[type == "estimated"]$runtime
    catf("  Parallel : %s", ps(max(vnapply(split(rt, lpt(rt, n)), sum)), nc = nc))
  }
  catf("  Total    : %s", ps(total, nc = nc))
}
