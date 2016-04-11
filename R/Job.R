Job = R6Class("Job",
  public = list(
    initialize = function(cache, id, pars, seed, resources) {
      self$cache = cache
      self$id = id
      self$job.pars = pars
      self$seed = seed
      self$resources = resources
    },
    id = NULL,
    job.pars = NULL,
    seed = NULL,
    resources = NULL,
    cache = NULL
  ),
  active = list(
    pars = function() c(self$job.pars, self$cache$get("more.args")),
    fun = function() self$cache$get("user.function")
  ),
  cloneable = FALSE
)

Experiment = R6Class("Experiment",
  public = list(
    initialize = function(cache, id, pars, repl, seed, resources, prob.name, algo.name) {
      self$cache = cache
      self$id = id
      self$pars = pars
      self$repl = repl
      self$seed = seed
      self$resources = resources
      self$prob.name = prob.name
      self$algo.name = algo.name
    },
    id = NULL,
    pars = NULL,
    repl = NULL,
    seed = NULL,
    resources = NULL,
    cache = NULL,
    prob.name = NULL,
    algo.name = NULL
  ),
  active = list(
    problem = function() self$cache$get(id = "..problem..", file.path("problems", self$prob.name)),
    algorithm = function() self$cache$get(file.path("algorithms", self$algo.name)),
    instance = function() {
      p = self$problem
      seed = if (is.null(p$seed)) self$seed else p$seed + self$repl - 1L
      wrapper = function(...) p$fun(job = self, data = p$data, ...)
      with_seed(seed, do.call(wrapper, self$pars$prob.pars))
    }
  ),
  cloneable = FALSE
)

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
#' \code{\link{ExperimentRegistry}} has the slots \dQuote{prob.name}, \dQuote{problem}, \dQuote{algo.name}, \dQuote{algorithm},
#' \dQuote{repl} (replication) and \dQuote{instance}.
#'
#' Note that the slots \dQuote{pars}, \dQuote{fun}, \dQuote{algorithm} and \dQuote{problem} and \dQuote{instance}
#' are actually functions which load required files from the file system and construct the object on the first access.
#' Each subsequent access returns a cached version from memory (except \dQuote{instance} which is not cached).
#'
#' Jobs can be executed with \code{\link{execJob}}.
#' @template id
#' @template reg
#' @return [\code{Job}].
#' @aliases Job
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' job = makeJob(1, reg = reg)
#' names(job)
makeJob = function(id, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}

#' @export
makeJob.Registry = function(id, reg = getDefaultRegistry()) {
  row = inner_join(filter(reg$status, id), reg$defs)
  Job$new(Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    resources = inner_join(row, reg$resources)$resources)
}

#' @export
makeJob.ExperimentRegistry = function(id, reg = getDefaultRegistry()) {
  row = inner_join(filter(reg$status, id), reg$defs)
  Experiment$new(Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    repl = row$repl, resources = inner_join(row, reg$resources)$resources, prob.name = row$problem, algo.name = row$algorithm)
}

getJob = function(jc, id, cache = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache = NULL) {
  row = filter(jc$defs, id)
  Job$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    resources = jc$resources)
}

getJob.ExperimentCollection = function(jc, id, cache = NULL) {
  row = filter(jc$defs, id)
  Experiment$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    repl = row$repl, resources = jc$resources, prob.name = row$problem, algo.name = row$algorithm)
}
