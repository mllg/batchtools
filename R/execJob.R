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
  if (".job" %chin% names(formals(job$fun))) {
    with_seed(job$seed, do.call(job$fun, c(job$pars, list(.job = job)), envir = .GlobalEnv))
  } else {
    with_seed(job$seed, do.call(job$fun, job$pars, envir = .GlobalEnv))
  }
}

#' @export
execJob.Experiment = function(job) {
  catf("Generating problem instance for problem '%s' ...", job$prob.name)
  instance = job$instance
  force(instance)
  job$allow.access.to.instance = FALSE

  catf("Applying algorithm '%s' on problem '%s' ...", job$algo.name, job$prob.name)
  wrapper = function(...) job$algorithm$fun(job = job, data = job$problem$data, instance = instance, ...)
  with_seed(job$seed, do.call(wrapper, job$pars$algo.pars, envir = .GlobalEnv))
}
