getResultPath = function(reg) {
  file.path(reg$file.dir, "results")
}
getLogPath = function(reg) {
  file.path(reg$file.dir, "logs")
}

getJobPath = function(reg) {
  file.path(reg$file.dir, "jobs")
}

getUpdatePath = function(reg) {
  file.path(reg$file.dir, "updates")
}

getExternalPath = function(reg) {
  file.path(reg$file.dir, "external")
}

getResultFiles = function(reg, ids) {
  file.path(reg$file.dir, "results", sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id))
}

getLogFiles = function(reg, ids, hash = ids$job.hash, log.file = ids$log.file) {
  file.path(reg$file.dir, "logs", ifelse(is.na(log.file), sprintf("%s.log", hash), log.file))
}

getJobFiles = function(reg, ids, hash = ids$job.hash) {
  file.path(reg$file.dir, "jobs", sprintf("%s.rds", hash))
}

getExternalDirs = function(reg, ids, dirs = ids$job.id) {
  file.path(reg$file.dir, "external", if (is.atomic(ids)) ids else ids$job.id)
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
