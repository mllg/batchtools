#' @title Execute a Single Jobs
#'
#' @description
#' Executes a single job (as created by \code{\link{makeJob}}) and returns
#' its result. Also works for Experiments.
#'
#' @param job [\code{\link{Job}} \code{\link{Experiment}}]\cr
#'   Job/Experiment to execute.
#' @return [any]. Result of the job.
#' @export
execJob = function(job) {
  UseMethod("execJob")
}

#' @export
execJob.character = function(job) {
  execJob(readRDS(job))
}

#' @export
execJob.Job = function(job) {
  with_seed(job$seed, do.call(job$fun, job$pars, envir = .GlobalEnv))
}

#' @export
execJob.Experiment = function(job) {
  catf("Generating problem instance for problem %s ...", job$prob.name)
  instance = job$instance

  catf("Applying algorithm %s on problem %s ...", job$algo.name, job$prob.name)
  wrapper = function(...) job$algorithm$fun(job = job, data = job$problem$data, instance = instance, ...)
  with_seed(job$seed, do.call(wrapper, job$pars$algo.pars, envir = .GlobalEnv))
}
