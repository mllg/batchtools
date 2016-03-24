readLog = function(id, impute = NULL, read.fun = readLines, reg = getDefaultRegistry()) {
  x = reg$status[id, c("job.id", "done", "job.hash"), with = FALSE, nomatch = 0L]
  log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", x$job.hash))

  if (is.na(x$job.hash) || !file.exists(log.file)) {
    if (!is.null(impute))
      return(impute)
    stopf("Log file for job with id %i not available", x$job.id)
  }

  lines = read.fun(log.file)
  pattern = sprintf("\\[job\\((chunk|%i)\\):", x$job.id)
  return(lines[!stri_startswith_fixed(lines, "[job") | stri_detect_regex(lines, pattern)])
}
