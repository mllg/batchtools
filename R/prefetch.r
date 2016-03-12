Cache = R6Class("Cache",
  cloneable = FALSE,
  public = list(
    cache = list(),
    file.dir = NA_character_,
    initialize = function(file.dir) {
      self$file.dir = file.dir
    },
    get = function(id, uri = id) {
      if (is.null(self$cache[[id]]) || self$cache[[id]]$uri != uri) {
        fn = file.path(self$file.dir, sprintf("%s.rds", uri))
        self$cache[[id]] = list(uri = uri, obj = if (file.exists(fn)) readRDS(fn) else NULL)
      }
      return(self$cache[[id]]$obj)
    }
  )
)

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
