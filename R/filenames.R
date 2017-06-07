Paths = R6Class("Paths", cloneable = FALSE,
  public = list(
    base = NA_character_,
    dir = character(0L),
    file = character(0L),
    initialize = function(file.dir) {
      self$base = normalizePath(file.dir, winslash = "/")
      dirs = c("results", "logs", "jobs", "updates", "external", "exports")
      self$dir = setNames(file.path(self$base, dirs), dirs)
    },
    results = function(ids) file.path(self$dir[["results"]], sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id)),
    external = function(ids) file.path(self$dir[["external"]], if (is.atomic(ids)) ids else ids$job.id),
    jobs = function(hash) file.path(self$dir[["jobs"]], sprintf("%s.rds", hash)),
    logs = function(ids, hash = ids$job.hash, log.file = ids$log.file) file.path(self$dir[["logs"]], ifelse(is.na(log.file), sprintf("%s.log", hash), log.file)),
    exports = function(name) file.path(self$dir[["exports"]], mangle(name))
  )
)

RegistryPaths = R6Class("RegistryPaths", cloneable = FALSE, inherit = Paths,
  public = list(
    initialize = function(file.dir) {
      super$initialize(file.dir)
      self$file["user.function"] = file.path(self$base, "user.function.rds")
      self$file["more.args"] = file.path(self$base, "more.args.rds")
    }
  )
)

ExperimentRegistryPaths = R6Class("ExperimentRegistryPaths", cloneable = FALSE, inherit = Paths,
  public = list(
    initialize = function(file.dir) {
      super$initialize(file.dir)
      self$dir[["problems"]] = file.path(self$base, "problems")
      self$dir[["algorithms"]] = file.path(self$base, "algorithms")
    },
    problem = function(name) file.path(self$dir[["problems"]], mangle(name)),
    algorithm = function(name) file.path(self$dir[["algorithms"]], mangle(name))
  )
)

setPaths = function(x) {
  UseMethod("setPaths")
}

setPaths.Registry = function(x) {
  x$paths = RegistryPaths$new(x$file.dir)
}

setPaths.ExperimentRegistry = function(x) {
  x$paths = ExperimentRegistryPaths$new(x$file.dir)
}

setPaths.JobCollection = function(x) {
  x$paths = RegistryPaths$new(x$file.dir)
}

setPaths.ExperimentCollection = function(x) {
  x$paths = ExperimentRegistryPaths$new(x$file.dir)
}

npath = function(path, must.work = TRUE) {
  if (stri_startswith_fixed(path, "~")) {
    # do not call normalizePath, we do not want to expand this paths relative to home
    if (must.work && !file.exists(path))
      stopf("Path '%s' not found", path)
    if (testOS("windows"))
      path = stri_replace_all_fixed(path, "\\", "/")
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = must.work)
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
