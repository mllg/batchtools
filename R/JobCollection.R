#' @title JobCollection Constructor
#'
#' @description
#' \code{makeJobCollection} takes multiple job ids and creates an object of class \dQuote{JobCollection} which holds all
#' necessary information for the calculation with \code{\link{doJobCollection}}. It is implemented as an environment
#' with the following variables:
#' \describe{
#'  \item{file.dir}{\code{file.dir} of the \link{Registry}.}
#'  \item{work.dir:}{\code{work.dir} of the \link{Registry}.}
#'  \item{job.hash}{Unique identifier of the job. Used to create names on the file system.}
#'  \item{jobs}{\code{\link[data.table]{data.table}} holding individual job information. See examples.}
#'  \item{log.file}{Location of the designated log file for this job.}
#'  \item{resources:}{Named list of of specified computational resources.}
#'  \item{uri}{Location of the job description file (saved with \code{link[base]{saveRDS}} on the file system.}
#'  \item{seed}{\code{integer(1)} Seed of the \link{Registry}.}
#'  \item{packages}{\code{character} with required packages to load via \code{\link[base]{require}}.}
#'  \item{namespaces}{code{character} with required packages to load via \code{\link[base]{requireNamespace}}.}
#'  \item{source}{\code{character} with list of files to source before execution.}
#'  \item{load}{\code{character} with list of files to load before execution.}
#'  \item{array.var}{\code{character(1)} of the array environment variable specified by the cluster functions.}
#'  \item{array.jobs}{\code{logical(1)} signaling if jobs were submitted using \code{chunks.as.arrayjobs}.}
#' }
#' If your \link{ClusterFunctions} uses a template, \code{\link[brew]{brew}} will be executed in the environment of such
#' a collection. Thus all variables available inside the job can be used in the template.
#'
#' @templateVar ids.default all
#' @template ids
#' @param resources [\code{list}]\cr
#'   Named list of resources. Default is \code{list()}.
#' @template reg
#' @return [\code{JobCollection}].
#' @family JobCollection
#' @aliases JobCollection
#' @rdname JobCollection
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE, packages = "methods")
#' batchMap(identity, 1:5, reg = tmp)
#'
#' # resources are usually set in submitJobs()
#' jc = makeJobCollection(1:3, resources = list(foo = "bar"), reg = tmp)
#' ls(jc)
#' jc$resources
makeJobCollection = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  UseMethod("makeJobCollection", reg)
}

createCollection = function(jobs, resources = list(), reg = getDefaultRegistry()) {
  jc              = new.env(parent = emptyenv())
  jc$jobs         = setkeyv(jobs, "job.id")
  jc$job.hash     = rnd_hash("job")
  jc$job.name     = if (anyMissing(jobs$job.name)) jc$job.hash else jobs$job.name[1L]
  jc$file.dir     = reg$file.dir
  jc$work.dir     = reg$work.dir
  jc$seed         = reg$seed
  jc$uri          = getJobFiles(reg, hash = jc$job.hash)
  jc$log.file     = fs::path(reg$file.dir, "logs", sprintf("%s.log", jc$job.hash))
  jc$packages     = reg$packages
  jc$namespaces   = reg$namespaces
  jc$source       = reg$source
  jc$load         = reg$load
  jc$resources    = resources
  jc$array.var    = reg$cluster.functions$array.var
  jc$array.jobs   = isTRUE(resources$chunks.as.arrayjobs)

  hooks = chintersect(names(reg$cluster.functions$hooks), batchtools$hooks$remote)
  if (length(hooks) > 0L)
    jc$hooks = reg$cluster.functions$hooks[hooks]
  return(jc)
}

#' @export
makeJobCollection.Registry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  jc = createCollection(mergedJobs(reg, convertIds(reg, ids), c("job.id", "job.name", "job.pars")), resources, reg)
  setClasses(jc, "JobCollection")
}

#' @export
makeJobCollection.ExperimentRegistry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  jc = createCollection(mergedJobs(reg, convertIds(reg, ids), c("job.id", "job.name", "problem", "algorithm", "prob.pars", "algo.pars", "repl")), resources, reg)
  setClasses(jc, c("ExperimentCollection", "JobCollection"))
}

#' @export
print.JobCollection = function(x, ...) {
  catf("Collection of %i jobs", nrow(x$jobs))
  catf("  Hash    : %s", x$job.hash)
  catf("  Log file: %s", x$log.file)
}
