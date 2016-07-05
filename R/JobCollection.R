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
#'  \item{log.file}{Location of the designated log file for this job.}
#'  \item{packages}{Vector of packages to load on the slaves, see \link{Registry}.}
#'  \item{resources:}{Named list of of specified computational resources.}
#'  \item{uri}{Location of the job description file (saved with \code{link[base]{saveRDS}} on the file system.}
#'  \item{defs}{\code{\link[data.table]{data.table}} holding individual job information. See examples.}
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
#' @aliases JobCollection
#' @rdname JobCollection
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' coll = makeJobCollection(1:3, reg = reg)
#' ls(coll)
#' coll$defs
#' coll$defs$pars
makeJobCollection = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  UseMethod("makeJobCollection", reg)
}

createCollection = function(ids, resources = list(), reg = getDefaultRegistry()) {
  jc            = new.env(parent = emptyenv())
  jc$debug      = getOption("batchtools.debug", FALSE)
  jc$file.dir   = reg$file.dir
  jc$work.dir   = reg$work.dir
  jc$seed       = reg$seed
  jc$job.hash   = digest::digest(list(runif(1L), Sys.time())) # random string
  jc$uri        = npath(reg$file.dir, "jobs", sprintf("%s.rds", jc$job.hash))
  jc$log.file   = npath(reg$file.dir, "logs", sprintf("%s.log", jc$job.hash))
  jc$packages   = reg$packages
  jc$namespaces = reg$namespaces
  jc$source     = reg$source
  jc$load       = reg$load
  jc$resources  = resources

  hooks = reg$cluster.functions$hooks
  if (length(hooks) > 0L) {
    hooks = hooks[intersect(names(hooks), batchtools$hooks[batchtools$hooks$remote]$name)]
    if (length(hooks) > 0L)
      jc$hooks = hooks
  }

  return(jc)
}

#' @export
makeJobCollection.Registry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  jc = createCollection(ids, resources, reg)
  jc$defs = inner_join(filter(reg$status, ids), reg$defs)[, c("job.id", "pars"), with = FALSE]
  setClasses(jc, "JobCollection")
}

#' @export
makeJobCollection.ExperimentRegistry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  jc = createCollection(ids, resources, reg)
  jc$defs = inner_join(filter(reg$status, ids), reg$defs)[, c("job.id", "pars", "problem", "algorithm", "repl"), with = FALSE]
  setClasses(jc, c("ExperimentCollection", "JobCollection"))
}

#' @export
print.JobCollection = function(x, ...) {
  catf("Collection of %i jobs", nrow(x$defs))
  catf("  Hash    : %s", x$job.hash)
  catf("  Log file: %s", x$log.file)
}
