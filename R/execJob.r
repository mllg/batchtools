execJob = function(job) {
  UseMethod("execJob")
}

execJob.Job = function(job) {
  withSeed(job$seed, do.call(job$fun, job$pars))
}

execJob.Experiment = function(job) {
  catf("Generating problem instance for problem %s ...", job$problem$name)
  wrapper = function(...) job$problem$fun(job = job, data = job$problem$data, ...)
  withSeed(job$problem$seed, instance <- do.call(wrapper, job$pars$prob.pars))

  catf("Applying algorithm %s on problem %s ...", job$algorithm$name, job$problem$name)
  wrapper = function(...) job$algorithm$fun(job = job, data = job$problem$data, problem = instance, ...)
  withSeed(job$seed, do.call(wrapper, job$pars$algo.pars))
}
