#' @title Computational Jobs
#'
#' @description
#' \code{makeJob} takes a single job id and creates an object of class
#' \dQuote{Job}. This job is passed to reduce functions like
#' \code{\link{reduceResults}} or to \code{\link{Algorithm}} for an
#' \code{\link{ExperimentRegistry}}. calculation with \code{\link{doJobs}}. It
#' is implemented as a list holding the following variables:
#' \describe{
#'  \item{job.id}{Job ID as integer.}
#'  \item{pars}{Job parameters. For \code{\link{ExperimentRegistry}}, this is a list with named elements \dQuote{prob.name}, \dQuote{prob.pars}, \dQuote{algo.name} and \dQuote{algo.pars}.}
#'  \item{seed}{Seed which is set via \code{\link{doJobs}}.}
#'  \item{resources}{Computational resources which were set for this job.}
#' }
#' Additionally, a regular \code{\link{Registry}} also has the slot
#' \dQuote{fun} with the user function while an
#' \code{\link{ExperimentRegistry}} has the slots \dQuote{problem} and
#' \dQuote{algorithm} (see \link{Problem} and \link{Algorithm}).
#'
#' @template id
#' @template reg
#' @return [\code{Job}].
#' @aliases Job
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' job = makeJob(1, reg = reg)
#' names(job)
makeJob = function(id, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}

#' @export
makeJob.Registry = function(id, reg = getDefaultRegistry()) {
  id = asIds(reg, id, n = 1L)
  cache = Cache(reg$file.dir)
  setClasses(list(
    job.id    = id$job.id,
    pars      = c(reg$status[list(id)][reg$defs, nomatch = 0L]$pars[[1L]], cache("more.args")),
    seed      = getSeed(reg$seed, id$job.id),
    resources = reg$status[list(id)][reg$resources, nomatch = 0L]$resources,
    fun       = cache("user.function")
  ), "Job")
}

#' @export
makeJob.ExperimentRegistry = function(id, reg = getDefaultRegistry()) {
  id = asIds(reg, id, n = 1L)
  cache = Cache(reg$file.dir)
  pars = reg$status[list(id)][reg$defs, on = "def.id", nomatch = 0L]$pars[[1L]]

  setClasses(list(
    job.id    = id$job.id,
    pars      = pars,
    seed      = getSeed(reg$seed, id$job.id),
    resources = reg$status[list(id)][reg$resources, nomatch = 0L]$resources,
    problem   = cache("prob/problem", file.path("problems", pars$prob.name)),
    algorithm = cache(paste0("algo/", pars$algo.name), file.path("algorithms", pars$algo.name))
  ), c("Experiment", "Job"))
}

#' @export
print.Job = function(x, ...) {
  catf("Job with id %i", x$job.id)
}

#' @export
print.Experiment = function(x, ...) {
  catf("Experiment with id %i", x$job.id)
}

getJob = function(jc, id, cache) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache) {
  j = jc$defs[list(id)]
  setClasses(list(
    job.id = j$job.id,
    pars = c(j$pars[[1L]], cache("more.args")),
    seed = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    fun = cache("user.function")
  ), "Job")
}

getJob.ExperimentCollection = function(jc, id, cache) {
  j = jc$defs[list(id)]
  pars = j$pars[[1L]]
  setClasses(list(
    job.id = j$job.id,
    pars = j$pars[[1L]],
    seed = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    problem = cache("prob/problem", file.path("problems", pars$prob.name)),
    algorithm = cache(paste0("algo/", pars$algo.name), file.path("algorithms", pars$algo.name))
  ), c("Experiment", "Job"))
}
