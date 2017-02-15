FileHandler = function(file.dir) {
  force(file.dir)
  list(
    result.dir = file.path(file.dir, "results"),
    log.dir = file.path(file.dir, "logs"),
    job.dir = file.path(file.dir, "jobs"),
    update.dir = file.path(file.dir, "updates"),
    external.dir = file.path(file.dir, "external"),
    results = function(ids) file.path(file.dir, "results", sprintf("%s.rds", if (is.atomic(ids)) ids else ids$job.id)),
    logs = function(ids = NULL, hash = ids$job.hash, log.file = ids$log.file) file.path(file.dir, "logs", ifelse(is.na(log.file), sprintf("%s.log", hash), log.file)),
    jobs = function(ids = NULL, hash = ids$job.hash) file.path(file.dir, "jobs", sprintf("%s.rds", hash)),
    external = function(ids = NULL) file.path(file.dir, "external", if (is.atomic(ids)) ids else ids$job.id)
  )
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
