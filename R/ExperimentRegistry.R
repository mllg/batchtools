#' @title Construct an Experiment Registry Object
#'
#' @description
#' \code{makeExperimentRegistry} constructs a special \code{\link{Registry}} which
#' is suitable for the definition of large scale computer experiments.
#'
#' Each experiments consists of a \code{\link{Problem}} and an \code{\link{Algorithm}}.
#' These can be parametrized with \code{\link{addExperiments}} to actually define computational
#' jobs.
#'
#' @param  ... [\code{ANY}]\cr
#'   Arguments passed to \code{\link{Registry}}.
#' @aliases ExperimentRegistry
#' @return [\code{ExperimentRegistry}].
#' @name ExperimentRegistry
#' @rdname ExperimentRegistry
#' @export
#' @examples
#' reg = makeTempExperimentRegistry(make.default = FALSE)
#' addProblem(reg = reg, "p1", fun = function(n, mean, sd, ...) rnorm(n, mean = mean, sd = sd))
#' addAlgorithm(reg = reg, "a1", fun = function(problem, ...) mean(problem))
#' addExperiments(reg = reg, list(p1 = CJ(n = 100, mean = -3:3, sd = 1:5)), list(a1 = data.table()))
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#'
#' # Reduce the results
#' reduceResults(reg = reg, fun = function(aggr, res, ...) c(aggr, res))
#'
#' # Join info table with results
#' ids = findDone(reg = reg)
#' tab = getJobPars(reg = reg, ids = ids)
#' res = reduceResultsDataTable(reg = reg, ids = ids)
#' tab[res]
makeExperimentRegistry = function(...) {
  reg = makeRegistry(...)

  dir.create(file.path(reg$file.dir, "problems"))
  dir.create(file.path(reg$file.dir, "algorithms"))

  reg$status$repl = integer(0L)
  reg$problems = character(0L)
  reg$algorithms = character(0L)

  setattr(reg, "class", c("ExperimentRegistry", class(reg)))
  saveRegistry(reg)
  return(reg)
}

#' @export
#' @inheritParams Registry
#' @rdname ExperimentRegistry
makeTempExperimentRegistry = function(make.default = FALSE, temp.dir = getOption("batchtools.temp.dir", tempdir()), ...) {
  makeExperimentRegistry(file.dir = file.path(temp.dir, basename(tempfile("registry"))), make.default = make.default, ...)
}

#' @export
print.ExperimentRegistry = function(x, ...) {
  catf("Experiment Registry")
  catf("  Number of jobs: %i", nrow(x$status))
  catf("  File dir: %s", x$file.dir)
  catf("  Work dir: %s", x$work.dir)
  catf("  Seed: %i", x$seed)
}

assertExperimentRegistry = function(reg, writeable = FALSE) {
  assertClass(reg, "ExperimentRegistry")
  if (writeable & !reg$writeable)
    stop("Registry must be writeable")
  invisible(TRUE)
}
