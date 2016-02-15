#' @title Computational Jobs
#'
#' @description
#' \code{makeJob} takes a single job id and creates an object of class \dQuote{Job}. This job is passed to reduce
#' functions like \code{\link{reduceResults}}. Furthermore, it can be used for an \code{\link{ExperimentRegistry}} while
#' in the user functions of each \code{\link{Problem}} and \code{\link{Algorithm}}. It is implemented as a list holding
#' the following variables:
#' \describe{
#'  \item{job.id}{Job ID as integer.}
#'  \item{pars}{Job parameters as named list. For \code{\link{ExperimentRegistry}}, the parameters are divided into the
#'    sublists \dQuote{prob.pars} and \dQuote{algo.pars}.}
#'  \item{seed}{Seed which is set via \code{\link{doJobCollection}}.}
#'  \item{resources}{Computational resources which were set for this job.}
#' }
#' Additionally, a regular \code{\link{Registry}} also has the slot \dQuote{fun} with the user function while an
#' \code{\link{ExperimentRegistry}} has the slots \dQuote{problem}, \dQuote{algorithm} and \dQuote{repl} (replication).
#'
#' Jobs can be executed with \code{\link{execJob}}.
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
  joined = inner_join(filter(reg$status, id), reg$defs)
  cache = Cache(reg$file.dir)
  setClasses(list(
    job.id    = joined$job.id,
    pars      = c(joined$pars[[1L]], cache("more.args")),
    seed      = getSeed(reg$seed, joined$job.id),
    resources = inner_join(joined, reg$resources)$resources,
    fun       = cache("user.function")
  ), "Job")
}

#' @export
makeJob.ExperimentRegistry = function(id, reg = getDefaultRegistry()) {
  joined = inner_join(filter(reg$status, id), reg$defs)
  cache = Cache(reg$file.dir)

  setClasses(list(
    job.id    = joined$job.id,
    pars      = joined$pars[[1L]],
    repl      = joined$repl,
    seed      = getSeed(reg$seed, joined$job.id),
    resources = inner_join(joined, reg$resources)$resources,
    problem   = cache("prob/problem", file.path("problems", joined$problem)),
    algorithm = cache(paste0("algo/", joined$algorithm), file.path("algorithms", joined$algorithm))
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
  j = filter(jc$defs, id)
  setClasses(list(
    job.id = j$job.id,
    pars = c(j$pars[[1L]], cache("more.args")),
    seed = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    fun = cache("user.function")
  ), "Job")
}

getJob.ExperimentCollection = function(jc, id, cache) {
  j = filter(jc$defs, id)
  setClasses(list(
    job.id = j$job.id,
    pars = j$pars[[1L]],
    seed = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    problem = cache("prob/problem", file.path("problems", j$problem)),
    algorithm = cache(paste0("algo/", j$algorithm), file.path("algorithms", j$algorithm)),
    repl = j$repl
  ), c("Experiment", "Job"))
}
