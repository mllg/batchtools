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
#' @param cache [\code{Cache}]\cr
#'  Internal object used to retrieve objects from the file system.
#' @template reg
#' @return [\code{Job}].
#' @aliases Job
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' job = makeJob(1, reg = reg)
#' names(job)
makeJob = function(id, cache = NULL, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}

#' @export
makeJob.Registry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  if (is.null(cache))
    cache = Cache$new(reg$file.dir)
  joined = inner_join(filter(reg$status, id), reg$defs)
  setClasses(list(
    job.id    = joined$job.id,
    pars      = c(joined$pars[[1L]], cache$get("more.args")),
    seed      = getSeed(reg$seed, joined$job.id),
    resources = inner_join(joined, reg$resources)$resources,
    fun       = cache$get("user.function")
  ), "Job")
}

#' @export
makeJob.ExperimentRegistry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  if (is.null(cache))
    cache = Cache$new(reg$file.dir)
  joined = inner_join(filter(reg$status, id), reg$defs)

  setClasses(list(
    job.id    = joined$job.id,
    pars      = joined$pars[[1L]],
    repl      = joined$repl,
    seed      = getSeed(reg$seed, joined$job.id),
    resources = inner_join(joined, reg$resources)$resources,
    problem   = cache$get("prob/problem", file.path("problems", joined$problem)),
    algorithm = cache$get(stri_join("algo/", joined$algorithm), file.path("algorithms", joined$algorithm))
  ), c("Experiment", "Job"))
}

getJob = function(jc, id, cache = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache = NULL) {
  if (is.null(cache))
    cache = Cache$new(jc$file.dir)
  j = filter(jc$defs, id)
  setClasses(list(
    job.id    = j$job.id,
    pars      = c(j$pars[[1L]], cache$get("more.args")),
    seed      = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    fun       = cache$get("user.function")
  ), "Job")
}

getJob.ExperimentCollection = function(jc, id, cache = NULL) {
  if (is.null(cache))
    cache = Cache$new(jc$file.dir)
  j = filter(jc$defs, id)
  setClasses(list(
    job.id    = j$job.id,
    pars      = j$pars[[1L]],
    seed      = getSeed(jc$seed, j$job.id),
    resources = jc$resources,
    problem   = cache$get("prob/problem", file.path("problems", j$problem)),
    algorithm = cache$get(stri_join("algo/", j$algorithm), file.path("algorithms", j$algorithm)),
    repl      = j$repl
  ), c("Experiment", "Job"))
}

#' @export
print.Job = function(x, ...) {
  catf("Job with id %i", x$job.id)
  catf("  Parameters: %i", length(x$pars))
  catf("  Seed      : %i", x$seed)
}

#' @export
print.Experiment = function(x, ...) {
  catf("Experiment with id %i", x$job.id)
  catf("  Problem  : %s (%i parameters)", x$problem$name, length(x$pars$prob.pars))
  catf("  Algorithm: %s (%i parameters)", x$algorithm$name, length(x$pars$algo.pars))
  catf("  Seed     : %i", x$seed)
}
