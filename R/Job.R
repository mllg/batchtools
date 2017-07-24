BaseJob = R6Class("BaseJob", cloneable = FALSE,
  public = list(
    file.dir   = NULL,
    id         = NULL,
    job.pars   = NULL,
    seed       = NULL,
    resources  = NULL,
    reader     = NULL,
    initialize = function(file.dir, reader, id, pars, seed, resources) {
      self$file.dir  = file.dir
      self$reader    = reader
      self$id        = id
      self$job.pars  = pars
      self$seed      = seed
      self$resources = resources
    }
  ),

  active = list(
    external.dir = function() {
      path = fp(self$file.dir, "external", self$id)
      dir.create(path, recursive = TRUE, showWarnings = FALSE)
      path
    }
  )
)

Job = R6Class("Job", cloneable = FALSE, inherit = BaseJob,
  public = list(
    initialize = function(file.dir, reader, id, pars, seed, resources) {
      super$initialize(file.dir, reader, id, pars, seed, resources)
    }
  ),
  active = list(
    fun = function() {
      self$reader$get(fp(self$file.dir, "user.function.rds"))
    },
    pars = function() {
      c(self$job.pars, self$reader$get(fp(self$file.dir, "more.args.rds")))
    }
  )
)


Experiment = R6Class("Experiment", cloneable = FALSE, inherit = BaseJob,
  public = list(
    repl = NA_integer_,
    prob.name = NA_character_,
    algo.name = NA_character_,
    allow.access.to.instance = TRUE,
    initialize = function(file.dir, reader, id, pars, repl, seed, resources, prob.name, algo.name) {
      super$initialize(file.dir, reader, id, pars, seed, resources)
      self$repl = repl
      self$prob.name = as.character(prob.name)
      self$algo.name = as.character(algo.name)
    }
  ),
  active = list(
    problem = function()  {
      self$reader$get(getProblemURI(self, self$prob.name), slot = "..problem..")
    },
    algorithm = function() {
      self$reader$get(getAlgorithmURI(self, self$algo.name))
    },
    pars = function() {
      self$job.pars
    },
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
#' @param reader [\code{RDSReader} | \code{NULL}]\cr
#'  Reader object to retrieve files. Used internally to cache reading from the file system.
#'  The default (\code{NULL}) does not make use of caching.
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
makeJob = function(id, reader = NULL, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}


#' @export
makeJob.Registry = function(id, reader = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "resource.id"))
  resources = reg$resources[row, "resources", on = "resource.id", nomatch = NA]$resources[[1L]] %??% list()
  Job$new(file.dir = reg$file.dir, reader %??% RDSReader$new(FALSE), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    resources = resources)
}

#' @export
makeJob.ExperimentRegistry = function(id, reader = NULL, reg = getDefaultRegistry()) {
  row = mergedJobs(reg, convertId(reg, id), c("job.id", "pars", "problem", "algorithm", "repl", "resource.id"))
  resources = reg$resources[row, "resources", on = "resource.id", nomatch = NA]$resources[[1L]] %??% list()
  Experiment$new(file.dir = reg$file.dir, reader %??% RDSReader$new(FALSE), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    repl = row$repl, resources = resources, prob.name = row$problem, algo.name = row$algorithm)
}

getJob = function(jc, i, reader = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, i, reader = RDSReader$new(FALSE)) {
  row = jc$jobs[i]
  Job$new(file.dir = jc$file.dir, reader = reader, id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id), resources = jc$resources)
}

getJob.ExperimentCollection = function(jc, i, reader = RDSReader$new(FALSE)) {
  row = jc$jobs[i]
  Experiment$new(file.dir = jc$file.dir, reader = reader, id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    repl = row$repl, resources = jc$resources, prob.name = row$problem, algo.name = row$algorithm)
}
