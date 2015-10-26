summarizeExperiments = function(ids = NULL, by = c("problem", "algorithm"), reg = getDefaultRegistry()) {
  pars = !setequal(by, c("problem", "algorithm"))
  getJobInfo(ids = ids, pars.as.cols = pars, reg = reg)[, list(.count = .N), by = by]
}
