execJob = function(jd, i, cache) {
  UseMethod("execJob")
}

execJob.JobDescription = function(jd, i, cache) {
  j = jd$defs[i]
  fun = cache("user.function")
  withSeed(getSeed(jd$seed, j$job.id), do.call(fun, c(j$pars[[1L]], cache("more.args"))))
}

execJob.ExperimentDescription = function(jd, i, cache) {
  j = jd$defs[i]
  pars = j$pars[[1L]]
  job = c(mget(c("file.dir", "job.hash", "resources"), jd),
          list(job.id = j$job.id, problem = pars$prob.name, algorithm = pars$algo.name))

  catf("Generating problem instance for problem %s ...", pars$prob.name)
  prob = cache("prob/problem", file.path("problems", pars$prob.name))
  wrapper = function(...) prob$fun(job = job, data = prob$data, ...)
  withSeed(prob$seed, instance <- do.call(wrapper, pars$prob.pars))

  catf("Applying algorithm %s on problem %s ...", pars$algo.name, pars$prob.name)
  algo = cache(paste0("algo/", pars$algo.name), file.path("algorithms", pars$algo.name))
  wrapper = function(...) algo$fun(job = job, data = prob$data, problem = instance, ...)
  withSeed(getSeed(jd$seed, j$job.id), do.call(wrapper, pars$algo.pars))
}
