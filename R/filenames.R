FileHandler = R6Class("FileHandler",
  public = list(
    result.dir = NULL, log.dir = NULL, job.dir = NULL, update.dir = NULL, external.dir = NULL,
    initialize = function(file.dir) {
      self$result.dir = file.path(file.dir, "results")
      self$log.dir = file.path(file.dir, "logs")
      self$job.dir = file.path(file.dir, "jobs")
      self$update.dir = file.path(file.dir, "updates")
      self$external.dir = file.path(file.dir, "external")
    },
    results = function(ids) {
      file.path(self$result.dir, sprintf("%s.rds", if (is.atomic(ids)) ids else ids$job.id))
    },
    logs = function(ids = NULL, hash = ids$job.hash, log.file = ids$log.file) {
      file.path(self$log.dir, ifelse(is.na(log.file), sprintf("%s.log", hash), log.file))
    },
    jobs = function(ids = NULL, hash = ids$job.hash) {
      file.path(self$job.dir, sprintf("%s.rds", hash))
    },
    external = function(ids = NULL) {
      file.path(self$external.dir, if (is.atomic(ids)) ids else ids$job.id)
    }
  )
)

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
