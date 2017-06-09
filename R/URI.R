URI = R6Class("URI", cloneable = FALSE,
  public = list(
    base = NA_character_,
    path = character(0L),
    initialize = function(file.dir) {
      self$base = normalizePath(file.dir, winslash = "/")
      paths = c("results", "logs", "jobs", "updates", "external", "exports")
      self$path = setNames(file.path(self$base, paths), paths)
    },
    results = function(ids) file.path(self$path[["results"]], sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id)),
    external = function(ids) file.path(self$path[["external"]], if (is.atomic(ids)) ids else ids$job.id),
    jobs = function(hash) file.path(self$path[["jobs"]], sprintf("%s.rds", hash)),
    logs = function(ids, hash = ids$job.hash, log.file = ids$log.file) file.path(self$path[["logs"]], ifelse(is.na(log.file), sprintf("%s.log", hash), log.file)),
    exports = function(name) file.path(self$path[["exports"]], mangle(name))
  )
)

RegistryURI = R6Class("RegistryURI", cloneable = FALSE, inherit = URI,
  public = list(
    files = character(0L),
    initialize = function(file.dir) {
      super$initialize(file.dir)
      files = c("user.function", "more.args")
      self$files = setNames(file.path(self$base, sprintf("%s.rds", files)), files)
    }
  )
)

ExperimentURI = R6Class("ExperimentURI", cloneable = FALSE, inherit = URI,
  public = list(
    initialize = function(file.dir) {
      super$initialize(file.dir)
      self$path[["problems"]] = file.path(self$base, "problems")
      self$path[["algorithms"]] = file.path(self$base, "algorithms")
    },
    problem = function(name) file.path(self$path[["problems"]], mangle(name)),
    algorithm = function(name) file.path(self$path[["algorithms"]], mangle(name))
  )
)

setURI = function(x) {
  UseMethod("setURI")
}

setURI.Registry = function(x) {
  x$uri = RegistryURI$new(x$file.dir)
}

setURI.ExperimentRegistry = function(x) {
  x$uri = ExperimentURI$new(x$file.dir)
}

setURI.JobCollection = function(x) {
  x$uri = RegistryURI$new(x$file.dir)
}

setURI.ExperimentCollection = function(x) {
  x$uri = ExperimentURI$new(x$file.dir)
}
