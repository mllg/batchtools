Cache = R6Class("Cache",
  cloneable = FALSE,
  public = list(
    cache = list(),
    file.dir = NA_character_,
    initialize = function(file.dir) {
      self$file.dir = file.dir
    },

    get = function(id, slot = id, subdir = "", mangle = FALSE) {
      if (is.null(self$cache[[slot]]) || self$cache[[slot]]$id != id) {
        path = file.path(self$file.dir, subdir, sprintf("%s.rds", if (mangle) digest(id) else id))
        self$cache[[slot]] = list(id = id, obj = if (file.exists(path)) readRDS(path) else NULL)
      }
      return(self$cache[[slot]]$obj)
    }
  )
)

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
    fun = function() self$cache$get("user.function"),
    external.dir = function() {
      path = file.path(self$cache$file.dir, "external", self$id)
      dir.create(path, recursive = TRUE, showWarnings = FALSE)
      path
    }
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
    problem = function() self$cache$get(id = self$prob.name, subdir = "problems", slot = "..problem..", mangle = TRUE),
    algorithm = function() self$cache$get(id = self$algo.name, subdir = "algorithms", mangle = TRUE),
    instance = function() {
      if (!self$allow.access.to.instance)
        stop("You cannot access 'job$instance' in the problem generation or algorithm function")
      p = self$problem
      seed = if (is.null(p$seed)) self$seed else p$seed + self$repl - 1L
      wrapper = function(...) p$fun(job = self, data = p$data, ...)
      with_seed(seed, do.call(wrapper, self$pars$prob.pars, envir = .GlobalEnv))
    },
    external.dir = function() {
      path = file.path(self$cache$file.dir, "external", self$id)
      dir.create(path, recursive = TRUE, showWarnings = FALSE)
      path
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
#'  \item{\code{pars}}{
#'    Job parameters as named list.
#'    For \code{\link{ExperimentRegistry}}, the parameters are divided into the sublists \dQuote{prob.pars} and \dQuote{algo.pars}.
#'  }
#'  \item{\code{seed}}{Seed which is set via \code{\link{doJobCollection}} as scalar integer.}
#'  \item{\code{resources}}{Computational resources which were set for this job as named list.}
#'  \item{\code{external.dir}}{
#'    Path to a directory which is created exclusively for this job. You can store external files here.
#'    Directory is persistent between multiple restarts of the job and can be cleaned by calling \code{\link{resetJobs}}.
#'  }
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
#' The realizations are cached for all slots except \dQuote{instance} (which might be stochastic).
#'
#' Jobs and Experiments can be executed manually with \code{\link{execJob}}.
#'
#' @template id
#' @param cache [\code{Cache} | \code{NULL}]\cr
#'  Cache to retrieve files. Used internally.
#' @template reg
#' @return [\code{Job} | \code{Experiment}].
#' @aliases Job Experiment
#' @rdname JobExperiment
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x, y) x + y, x = 1:2, more.args = list(y = 99), reg = tmp)
#' submitJobs(resources = list(foo = "bar"), reg = tmp)
#' job = makeJob(1, reg = tmp)
#' print(job)
#'
#' # Get the parameters:
#' job$pars
#'
#' # Get the job resources:
#' job$resources
#'
#' # Execute the job locally:
#' execJob(job)
makeJob = function(id, cache = NULL, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}


#' @export
makeJob.Registry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "resource.id"))
  resources = reg$resources[row, "resources", on = "resource.id", nomatch = NA, with = FALSE]$resources[[1L]] %??% list()
  Job$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    resources = resources)
}

#' @export
makeJob.ExperimentRegistry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "problem", "algorithm", "repl", "resource.id"))
  resources = reg$resources[row, "resources", on = "resource.id", nomatch = NA, with = FALSE]$resources[[1L]] %??% list()
  Experiment$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    repl = row$repl, resources = resources, prob.name = row$problem, algo.name = row$algorithm)
}

getJob = function(jc, id, cache = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache = NULL) {
  row = filter(jc$jobs, id)
  Job$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    resources = jc$resources)
}

getJob.ExperimentCollection = function(jc, id, cache = NULL) {
  row = filter(jc$jobs, id)
  Experiment$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    repl = row$repl, resources = jc$resources, prob.name = row$problem, algo.name = row$algorithm)
}
