#' @title Inspect Log Files
#'
#' @description
#' Opens a pager to the stored log file. For customization, see \code{\link[base]{file.show}}.
#' @template id
#' @template reg
#' @export
#' @family debug
#' @return Nothing.
showLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  id = assertJobIds(asJobIds(reg, id), single.id = TRUE)
  lines = readLog(id, reg = reg)

  log.file = file.path(tempdir(), sprintf("%i.log", id$job.id))
  writeLines(text = lines, con = log.file)
  file.show(log.file, delete.file = TRUE)
}

readLog = function(id, impute = NULL, reg = getDefaultRegistry()) {
  x = reg$status[id, c("job.id", "done", "job.hash"), with = FALSE, nomatch = 0L]
  log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", x$job.hash))
  if (!file.exists(log.file)) {
    if (is.null(impute))
      stopf("Log file for job with id %i not found", x$job.id)
    return(impute)
  }

  lines = readLines(log.file)
  pattern = sprintf("\\[job\\((chunk|%i)\\):", x$job.id)
  lines[!stri_startswith_fixed(lines, "[job") | stri_detect_regex(lines, pattern)]
}
