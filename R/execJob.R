#' @title Execute a Single Jobs
#'
#' @description
#' Executes a single job (as created by \code{\link{makeJob}}) and returns
#' its result. Also works for Experiments.
#'
#' @param job [\code{\link{Job}} | \code{\link{Experiment}}]\cr
#'   Job/Experiment to execute.
#' @return Result of the job.
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:2, reg = tmp)
#' job = makeJob(1, reg = tmp)
#' execJob(job)
execJob = function(job) {
  UseMethod("execJob")
}

#' @export
execJob.character = function(job) {
  execJob(readRDS(job))
}

#' @export
execJob.JobCollection = function(job) {
  if (nrow(job$jobs) != 1L)
    stop("You must provide a JobCollection with exactly one job")
  execJob(getJob(job, i = 1L))
}

#' @export
execJob.Job = function(job) {
  local_options(list(error = function(e) traceback(2L)))
  messagef("### [bt%s]: Setting seed to %i ...", now(), job$id, job$seed)
  if (".job" %chin% names(formals(job$fun))) {
    with_seed(job$seed, do.call(job$fun, c(job$pars, list(.job = job)), envir = .GlobalEnv))
  } else {
    with_seed(job$seed, do.call(job$fun, job$pars, envir = .GlobalEnv))
  }
}

#' @export
execJob.Experiment = function(job) {
  local_options(list(error = function(e) traceback(2L)))
  messagef("### [bt%s]: Generating problem instance for problem '%s' ...", now(), job$prob.name)
  instance = job$instance
  force(instance)
  job$allow.access.to.instance = FALSE

  wrapper = function(...) job$algorithm$fun(job = job, data = job$problem$data, instance = instance, ...)
  messagef("### [bt%s]: Applying algorithm '%s' on problem '%s' for job %i (seed = %i) ...", now(), job$algo.name, job$prob.name, job$id, job$seed)
  with_seed(job$seed, do.call(wrapper, job$algo.pars, envir = .GlobalEnv))
}
