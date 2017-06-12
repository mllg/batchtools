npath = function(path, must.work = TRUE) {
  if (stri_startswith_fixed(path, "~")) {
    # do not call normalizePath, we do not want to expand this paths relative to home
    if (must.work && !file.exists(path))
      stopf("File '%s' not found", path)
    if (testOS("windows"))
      path = stri_replace_all_fixed(path, "\\", "/")
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = must.work)
}

getResultPath = function(reg) {
  file.path(path.expand(reg$file.dir), "results")
}

getLogPath = function(reg) {
  file.path(path.expand(reg$file.dir), "logs")
}

getJobPath = function(reg) {
  file.path(path.expand(reg$file.dir), "jobs")
}

getUpdatePath = function(reg) {
  file.path(path.expand(reg$file.dir), "updates")
}

getExternalPath = function(reg) {
  file.path(path.expand(reg$file.dir), "external")
}

getResultFiles = function(reg, ids) {
  file.path(path.expand(reg$file.dir), "results", sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id))
}

getLogFiles = function(reg, ids, hash = ids$job.hash, log.file = ids$log.file) {
  file.path(path.expand(reg$file.dir), "logs", ifelse(is.na(log.file), sprintf("%s.log", hash), log.file))
}

getJobFiles = function(reg, ids, hash = ids$job.hash) {
  file.path(path.expand(reg$file.dir), "jobs", sprintf("%s.rds", hash))
}

getExternalDirs = function(reg, ids, dirs = ids$job.id) {
  file.path(path.expand(reg$file.dir), "external", if (is.atomic(ids)) ids else ids$job.id)
}

getProblemURI = function(file.dir, name) {
  file.path(path.expand(file.dir), "problems", mangle(name))
}

getAlgorithmURI = function(file.dir, name) {
  file.path(path.expand(file.dir), "algorithms", mangle(name))
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
