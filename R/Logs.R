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
#' @param ignore.case [\code{logical(1L)}]\cr
#'  If \code{TRUE} the match will be performed case insensitive.
#' @template reg
#' @export
#' @family debug
#' @return [\code{\link{data.table}}]. Matching job ids are stored in the column \dQuote{job.id}.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
grepLogs = function(ids = NULL, pattern = "", ignore.case = FALSE, reg = getDefaultRegistry()) {
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

  assertRegistry(reg, sync = TRUE)
  assertString(pattern, na.ok = TRUE)
  assertFlag(ignore.case)

  ids = filter(reg$status, ids %??% .findStarted(reg = reg))[, c("job.id", "job.hash"), with = FALSE]
  if (is.na(pattern) || !nzchar(pattern))
    return(ids[, "job.id", with = FALSE])
  setorderv(ids, "job.hash")
  reader = Reader()

  found = logical(nrow(ids))
  matches = character(nrow(ids))
  for (i in seq_row(ids)) {
    lines = readLog(ids$job.id[i], impute = NA_character_, reg = reg, read.fun = reader)
    if (!testScalarNA(lines)) {
      m = stri_detect_regex(lines, pattern, case_insensitive = ignore.case)
      if (any(m)) {
        found[i] = TRUE
        matches[i] = stri_join(lines[m], collapse = "\n")
      }
    }
  }

  res = cbind(ids[found, "job.id", with = FALSE], data.table(matches = matches[found]))
  setkeyv(res, "job.id")[]
}

#' @title Inspect Log Files
#'
#' @description
#' \code{showLog} opens the log in the pager. For customization, see \code{\link[base]{file.show}}.
#' \code{getLog} returns the log as character vector.
#' @template id
#' @template reg
#' @export
#' @family debug
#' @return Nothing.
showLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  id = asJobTable(reg, id, single.id = TRUE)
  lines = readLog(id, reg = reg)
  log.file = file.path(tempdir(), sprintf("%i.log", id$job.id))
  writeLines(text = lines, con = log.file)
  file.show(log.file, delete.file = TRUE)
}

#' @export
#' @rdname showLog
getLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  id = asJobTable(reg, id, single.id = TRUE)
  readLog(id, reg = reg)
}
