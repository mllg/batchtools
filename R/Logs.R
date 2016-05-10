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

#' @title Grep Log Files for a Pattern
#'
#' @description
#' Greps through log files and reports jobs with matches.
#' @templateVar ids.default findStarted
#' @template ids
#' @param pattern [\code{character(1L)}]\cr
#'  Regular expression.
#' @template reg
#' @export
#' @family debug
#' @return [\code{data.table}]. Matching job ids are stored in the column \dQuote{job.id}.
grepLogs = function(ids = NULL, pattern = "", reg = getDefaultRegistry()) {
  Reader = function() {
    last.fn = NA_character_
    lines = NA_character_
    function(fn) {
      if (is.na(last.fn) || fn != last.fn) {
        last.fn <<- fn
        lines <<- readLines(fn)
      }
      return(lines)
    }
  }

  assertRegistry(reg)
  syncRegistry(reg)
  ids = filter(reg$status, ids %??% .findStarted(reg = reg))[, c("job.id", "job.hash"), with = FALSE]
  if (is.na(pattern) || !nzchar(pattern))
    return(ids[, "job.id", with = FALSE])
  setorderv(ids, "job.hash")

  assertString(pattern)
  reader = Reader()

  found = logical(nrow(ids))
  matches = character(nrow(ids))
  for (i in seq_row(ids)) {
    lines = readLog(ids$job.id[i], impute = NA_character_, reg = reg, read.fun = reader)
    if (!testScalarNA(lines)) {
      m = stri_detect_regex(lines, pattern)
      if (any(m)) {
        found[i] = TRUE
        matches[i] = stri_join(lines[m], collapse = "\n")
      }
    }
  }

  res = cbind(ids[found, "job.id", with = FALSE], data.table(matches = matches[found]))
  setkeyv(res, "job.id")
  return(res)
}

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
  id = asJobTable(reg, id)
  if (nrow(id) != 1L)
    stopf("You must provide exactly 1 id (%i provided)", nrow(id))
  lines = readLog(id, reg = reg)

  log.file = file.path(tempdir(), sprintf("%i.log", id$job.id))
  writeLines(text = lines, con = log.file)
  file.show(log.file, delete.file = TRUE)
}
