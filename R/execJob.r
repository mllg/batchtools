execJob = function(job) {
  UseMethod("execJob")
}

execJob.Job = function(job) {
  with_seed(job$seed, do.call(job$fun, job$pars))
}

execJob.Experiment = function(job) {
  catf("Generating problem instance for problem %s ...", job$problem$name)
  if (is.null(job$problem$seed)) {
    prob.seed = job$seed
  } else {
    prob.seed = job$problem$seed + job$repl - 1L
    catf("Using problem seed %i ...", prob.seed)
  }
  wrapper = function(...) job$problem$fun(job = job, data = job$problem$data, ...)
  with_seed(prob.seed, instance <- do.call(wrapper, job$pars$prob.pars))

  catf("Applying algorithm %s on problem %s ...", job$algorithm$name, job$problem$name)
  wrapper = function(...) job$algorithm$fun(job = job, data = job$problem$data, instance = instance, ...)
  with_seed(job$seed, do.call(wrapper, job$pars$algo.pars))
}
