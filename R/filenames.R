getResultPath = function(reg = getDefaultRegistry()) {
  file.path(reg$file.dir, "results")
}
getLogPath = function(reg = getDefaultRegistry()) {
  file.path(reg$file.dir, "logs")
}

getJobPath = function(reg = getDefaultRegistry()) {
  file.path(reg$file.dir, "jobs")
}

getUpdatePath = function(reg = getDefaultRegistry()) {
  file.path(reg$file.dir, "updates")
}

getExternalPath = function(reg = getDefaultRegistry()) {
  file.path(reg$file.dir, "external")
}

getResultFiles = function(fd, ids) {
  file.path(fd, "results", sprintf("%i.rds", ids))
}

getLogFiles = function(fd, hash) {
  file.path(fd, "logs", sprintf("%s.log", hash))
}

getJobFiles = function(fd, hash) {
  file.path(fd, "jobs", sprintf("%s.rds", hash))
}

getUpdateFiles = function(fd, hash, num = 0L) {
  file.path(fd, "updates", sprintf("%s-%i.rds", hash, num))
}

getExternalDirs = function(fd, dirs) {
  file.path(fd, "external", as.character(dirs))
}
