readLog = function(id, impute = NULL, read.fun = readLines, reg = getDefaultRegistry()) {
  lognotfound = function(...) {
    if (!is.null(impute))
      return(impute)
    stopf(...)
  }
  x = reg$status[id, c("job.id", "done", "job.hash"), with = FALSE, nomatch = 0L]
  if (is.na(x$job.hash))
    return(lognotfound("Log file for job with id %i not available", x$job.id))

  if (is.null(reg$cluster.functions$readLog)) {
    log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", x$job.hash))
    if (!file.exists(log.file))
      return(lognotfound("Log file for job with id %i not found", x$job.id))
    lines = read.fun(log.file)
  } else {
    lines = reg$cluster.functions$readLog(reg, x$job.hash)
    if (is.null(lines))
      return(lognotfound("Log file for job with id %i not found", x$job.id))
  }
  pattern = sprintf("\\[job\\((chunk|%i)\\):", x$job.id)
  return(lines[!stri_startswith_fixed(lines, "[job") | stri_detect_regex(lines, pattern)])
}
