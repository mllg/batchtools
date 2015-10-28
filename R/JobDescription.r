#' @title Computational Jobs
#'
#' @description
#' \code{makeJobDescription} takes multiple ids and creates an object of class \dQuote{JobDescription}
#' which holds all necessary information for the calculation with \code{\link{doJobs}}.
#'
#' A job is an environment with the following variables:
#' \describe{
#'  \item{file.dir}{\code{file.dir} of the \link{Registry}.}
#'  \item{work.dir:}{\code{work.dir} of the \link{Registry}.}
#'  \item{job.hash}{Unique identifier of the job. Used to create names on the file system.}
#'  \item{log.file}{Location of the designated log file for this job.}
#'  \item{packages}{Vector of packages to load on the slaves, see \link{Registry}.}
#'  \item{resources:}{Named list of of specified computational resources.}
#'  \item{uri}{Location of the job description file (saved with \code{link[base]{saveRDS}} on the file system.}
#'  \item{defs}{\code{\link[data.table]{data.table}} holding individual job information.}
#'}
#' If your \link{ClusterFunctions} uses a template, \code{\link[brew]{brew}} will be executed
#' in the job's environment. Thus all variables available inside the job can be used in the template.
#'
#' @templateVar ids.default all
#' @template ids
#' @param resources [\code{list}]\cr
#'   Named list of resources. Default is \code{list()}.
#' @template reg
#' @return [\code{Job}]. See description.
#' @aliases JobDescription
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' jobs = makeJobDescription(1:3, reg = reg)
#' ls(jobs)
#' jobs$defs
makeJobDescription = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  UseMethod("makeJobDescription", reg)
}

#' @export
makeJobDescription.Registry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  ids = asIds(reg, ids, default = .findAll(reg = reg))

  jd            = new.env(parent = emptyenv())
  jd$file.dir   = reg$file.dir
  jd$work.dir   = reg$work.dir
  jd$seed       = reg$seed
  jd$job.hash   = getRandomString()
  jd$uri        = npath(reg$file.dir, "jobs", sprintf("%s.rds", jd$job.hash))
  jd$log.file   = npath(reg$file.dir, "logs", sprintf("%s.log", jd$job.hash))
  jd$packages   = reg$packages
  jd$namespaces = reg$namespaces
  jd$defs       = reg$status[ids][reg$defs, c("job.id", "pars"), on = "def.id", nomatch = 0L, with = FALSE]
  jd$resources  = resources

  setClasses(jd, "JobDescription")
}

#' @export
makeJobDescription.ExperimentRegistry = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  jd = NextMethod("makeJobDescription", object = reg)
  setClasses(jd, c("ExperimentDescription", "JobDescription"))
}

#' @export
print.JobDescription = function(x, ...) {
  catf("Collection of %i jobs", nrow(x$defs))
  catf("  Hash    : %s", x$job.hash)
  catf("  Log file: %s", x$log.file)
}
