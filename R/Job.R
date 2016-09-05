Job = R6Class("Job",
  cloneable = FALSE,
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
  )
)

Experiment = R6Class("Experiment",
  cloneable = FALSE,
  public = list(
    initialize = function(cache, id, pars, repl, seed, resources, prob.name, algo.name) {
      self$cache = cache
      self$id = id
      self$pars = pars
      self$repl = repl
      self$seed = seed
      self$resources = resources
      self$prob.name = as.character(prob.name)
      self$algo.name = as.character(algo.name)
    },
    id = NULL,
    pars = NULL,
    repl = NULL,
    seed = NULL,
    resources = NULL,
    cache = NULL,
    prob.name = NULL,
    algo.name = NULL,
    allow.access.to.instance = TRUE
  ),
  active = list(
    problem = function() self$cache$get(id = "..problem..", file.path("problems", self$prob.name)),
    algorithm = function() self$cache$get(file.path("algorithms", self$algo.name)),
    instance = function() {
      if (!self$allow.access.to.instance)
        stop("You cannot access 'job$instance' in the problem generation or algorithm function")
      p = self$problem
      seed = if (is.null(p$seed)) self$seed else p$seed + self$repl - 1L
      wrapper = function(...) p$fun(job = self, data = p$data, ...)
      with_seed(seed, do.call(wrapper, self$pars$prob.pars, envir = .GlobalEnv))
    }
  )
)

#' @title Jobs and Experiments
#'
#' @description
#' Jobs and Experiments are abstract objects which hold all information necessary to execute a single computational
#' job for a \code{\link{Registry}} or \code{\link{ExperimentRegistry}}, respectively.
#'
#' They can be created using the constructor \code{makeJob} which takes a single job id.
#' Jobs and Experiments are passed to reduce functions like \code{\link{reduceResults}}.
#' Furthermore, Experiments can be used in the functions of the \code{\link{Problem}} and \code{\link{Algorithm}}.
#' Jobs and Experiments hold these information:
#' \describe{
#'  \item{\code{job.id}}{Job ID as integer.}
#'  \item{\code{pars}}{Job parameters as named list. For \code{\link{ExperimentRegistry}}, the parameters are divided into the
#'    sublists \dQuote{prob.pars} and \dQuote{algo.pars}.}
#'  \item{\code{seed}}{Seed which is set via \code{\link{doJobCollection}}.}
#'  \item{\code{resources}}{Computational resources which were set for this job.}
#'  \item{\code{fun}}{Job only: User function passed to \code{\link{batchMap}}.}
#'  \item{\code{prob.name}}{Experiments only: Problem id.}
#'  \item{\code{algo.name}}{Experiments only: Algorithm id.}
#'  \item{\code{problem}}{Experiments only: \code{\link{Problem}}.}
#'  \item{\code{instance}}{Experiments only: Problem instance.}
#'  \item{\code{algorithm}}{Experiments only: \code{\link{Algorithm}}.}
#'  \item{\code{repl}}{Experiments only: Replication number.}
#' }
#'
#' Note that the slots \dQuote{pars}, \dQuote{fun}, \dQuote{algorithm} and \dQuote{problem}
#' lazy-load required files from the file system and construct the object on the first access.
#' Each subsequent access returns a cached version from memory (except \dQuote{instance} which is not cached).
#'
#' Jobs and Experiments be executed with \code{\link{execJob}}.
#' @template id
#' @param cache [\code{Cache}]\cr
#'  Cache to retrieve files. Used internally.
#' @template reg
#' @return [\code{Job} | \code{Experiment}].
#' @aliases Job Experiment
#' @rdname JobExperiment
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:5, reg = tmp)
#' job = makeJob(1, reg = tmp)
#' names(job)
makeJob = function(id, cache = NULL, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}

#' @export
makeJob.Registry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "resource.id"))
  Job$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    resources = filter(reg$resources, row)$resources)
}

#' @export
makeJob.ExperimentRegistry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "problem", "algorithm", "repl", "resource.id"))
  Experiment$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    repl = row$repl, resources = filter(reg$resources, row)$resources, prob.name = row$problem, algo.name = row$algorithm)
}

getJob = function(jc, id, cache = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache = NULL) {
  row = filter(jc$jobs, castIds(id))
  Job$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    resources = jc$resources)
}

getJob.ExperimentCollection = function(jc, id, cache = NULL) {
  row = filter(jc$jobs, castIds(id))
  Experiment$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    repl = row$repl, resources = jc$resources, prob.name = row$problem, algo.name = row$algorithm)
}
