
prefetch = function(jc, cache) {
  UseMethod("prefetch")
}

prefetch.JobCollection = function(jc, cache) {
  cache$get("user.fun")
  cache$get("more.args")
  invisible(TRUE)
}

prefetch.ExperimentCollection = function(jc, cache) {
  problems = unique(jc$defs$problem)
  if (length(problems) == 1L)
    cache$get("prob/problem", file.path("problems", problems))
  algorithms = unique(jc$defs$algorithm)
  Map(cache$get, id = stri_join("algo/", algorithms), uri = file.path("algorithms", algorithms))
  invisible(TRUE)
}
