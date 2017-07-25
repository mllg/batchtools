npath = function(path, must.work = TRUE) {
  if (any(stri_startswith_fixed(path, c("~", "$")))) {
    # do not call normalizePath, we do not want to expand this paths relative to home
    if (must.work && !file.exists(path))
      stopf("File '%s' not found", path)
    if (testOS("windows"))
      path = stri_replace_all_fixed(path, "\\", "/")
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = must.work)
}

fp = function(...) {
  file.path(..., fsep = "/")
}

dir = function(reg, what) {
  fp(normalizePath(reg$file.dir, winslash = "/"), what)
}

getResultFiles = function(reg, ids) {
  fp(dir(reg, "results"), sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id))
}

getLogFiles = function(reg, ids) {
  job.hash = log.file = NULL
  tab = reg$status[list(ids), c("job.id", "job.hash", "log.file")]
  tab[is.na(log.file) & !is.na(job.hash), log.file := sprintf("%s.log", job.hash)]
  tab[!is.na(log.file), log.file := fp(dir(reg, "logs"), log.file)]$log.file
}

getJobFiles = function(reg, hash) {
  fp(dir(reg, "jobs"), sprintf("%s.rds", hash))
}

getExternalDirs = function(reg, ids) {
  fp(dir(reg, "external"), if (is.atomic(ids)) ids else ids$job.id)
}

getProblemURI = function(reg, name) {
  fp(dir(reg, "problems"), mangle(name))
}

getAlgorithmURI = function(reg, name) {
  fp(dir(reg, "algorithms"), mangle(name))
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
