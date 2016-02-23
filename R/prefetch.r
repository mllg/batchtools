Cache = function(file.dir) {
  cache = new.env(parent = emptyenv())
  force(file.dir)
  force(cache)

  function(id, uri = id) {
    if (is.null(cache[[id]]) || cache[[id]]$uri != uri) {
      fn = file.path(file.dir, sprintf("%s.rds", uri))
      cache[[id]] = list(uri = uri, obj = if (file.exists(fn)) readRDS(fn) else NULL)
    }
    return(cache[[id]]$obj)
  }
}

prefetch = function(jc, cache) {
  UseMethod("prefetch")
}

prefetch.JobCollection = function(jc, cache) {
  cache("user.fun")
  cache("more.args")
  invisible(TRUE)
}

prefetch.ExperimentCollection = function(jc, cache) {
  problems = unique(jc$defs$problem)
  if (length(problems) == 1L)
    cache("prob/problem", file.path("problems", problems))
  algorithms = unique(jc$defs$algorithm)
  Map(cache, id = stri_join("algo/", algorithms), uri = file.path("algorithms", algorithms))
  invisible(TRUE)
}
