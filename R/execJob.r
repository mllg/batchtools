execJob = function(jd, i, cache) {
  UseMethod("execJob")
}

execJob.JobDescription = function(jd, id, cache) {
  j = jd$defs[.(id)]
  fun = cache("user.function")
  withSeed(getSeed(jd$seed, j$job.id), do.call(fun, c(j$pars[[1L]], cache("more.args"))))
}

execJob.ExperimentDescription = function(jd, id, cache) {
  subsetJD = function(jd, id) {
    list2env(c(mget(setdiff(ls(jd), "defs"), jd), parent = emptyenv(), list(defs = jd$defs[.(id)])), parent = emptyenv())
  }

  j = jd$defs[.(id)]
  pars = j$pars[[1L]]

  catf("Generating problem instance for problem %s ...", pars$prob.name)
  prob = cache("prob/problem", file.path("problems", pars$prob.name))
  wrapper = function(...) prob$fun(job = subsetJD(jd, id), data = prob$data, ...)
  withSeed(prob$seed, instance <- do.call(wrapper, pars$prob.pars))

  catf("Applying algorithm %s on problem %s ...", pars$algo.name, pars$prob.name)
  algo = cache(paste0("algo/", pars$algo.name), file.path("algorithms", pars$algo.name))
  wrapper = function(...) algo$fun(job = subsetJD(jd, id), data = prob$data, problem = instance, ...)
  withSeed(getSeed(jd$seed, j$job.id), do.call(wrapper, pars$algo.pars))
}
