#' @title ExperimentRegistry Constructor
#'
#' @description
#' \code{makeExperimentRegistry} constructs a special \code{\link{Registry}} which
#' is suitable for the definition of large scale computer experiments.
#'
#' Each experiments consists of a \code{\link{Problem}} and an \code{\link{Algorithm}}.
#' These can be parametrized with \code{\link{addExperiments}} to actually define computational
#' jobs.
#'
#' @inheritParams Registry
#' @aliases ExperimentRegistry
#' @return [\code{ExperimentRegistry}].
#' @name ExperimentRegistry
#' @rdname ExperimentRegistry
#' @export
#' @family Experiment
#' @examples
#' reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#' addProblem(reg = reg, "p1",
#'   fun = function(job, data, n, mean, sd, ...) rnorm(n, mean = mean, sd = sd))
#' addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) mean(instance))
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
makeExperimentRegistry = function(file.dir = "registry", work.dir = getwd(), conf.file = findConfFile("batchtools.conf.R"), packages = character(0L), namespaces = character(0L),
  source = character(0L), load = character(0L), seed = NULL, make.default = TRUE) {

  reg = makeRegistry(file.dir = file.dir, work.dir = work.dir, conf.file = conf.file,
    packages = packages, namespaces = namespaces, source = source, load = load, seed = seed, make.default = make.default)

  dir.create(file.path(reg$file.dir, "problems"))
  dir.create(file.path(reg$file.dir, "algorithms"))

  reg$status$repl = integer(0L)
  reg$defs$problem = factor(character(0L))
  reg$defs$algorithm = factor(character(0L))

  setattr(reg, "class", c("ExperimentRegistry", class(reg)))
  saveRegistry(reg)
  return(reg)
}

#' @export
print.ExperimentRegistry = function(x, ...) {
  catc("Experiment Registry")
  catf("  ClusterFunctions: %s", x$cluster.functions$name)
  catf("  File dir   : %s", x$file.dir)
  catf("  Work dir   : %s", x$work.dir)
  catf("  Jobs       : %i", nrow(x$status))
  catf("  Problems   : %i", nlevels(x$defs$problem))
  catf("  Algorithms : %i", nlevels(x$defs$algorithm))
  catf("  Seed       : %i", x$seed)
}

assertExperimentRegistry = function(reg, writeable = FALSE, sync = FALSE, running.ok = TRUE) {
  assertClass(reg, "ExperimentRegistry")
  assertRegistry(reg, writeable = writeable, sync = sync, running.ok = running.ok)
}
