#' @useDynLib batchtools fill_gaps
readLog = function(id, impute = NULL, reg = getDefaultRegistry()) {
  tab = reg$status[id, c("job.id", "done", "job.hash"), with = FALSE, nomatch = NA]
  log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", tab$job.hash))

  if (is.na(tab$job.hash) || !file.exists(log.file)) {
    if (!is.null(impute))
      return(impute)
    stopf("Log file for job with id %i not available", tab$job.id)
  }

  lines = readLines(log.file)
  job.id = as.integer(stri_match_last_regex(lines, c("\\[batchtools job\\.id=([0-9]+)\\]$"))[, 2L])

  setkeyv(data.table(
    job.id = .Call(fill_gaps, job.id),
    lines = lines
  ), "job.id", physical = FALSE)
}

extractLog = function(log, id) {
  job.id = NULL
  log[is.na(job.id) | job.id %in% id$job.id]$lines
}

#' @title Grep Log Files for a Pattern
#'
#' @description
#' Greps through log files and reports jobs with matches.
#' @templateVar ids.default findStarted
#' @template ids
#' @param pattern [\code{character(1L)}]\cr
#'  Regular expression or string (see \code{fixed}).
#' @param ignore.case [\code{logical(1L)}]\cr
#'  If \code{TRUE} the match will be performed case insensitive.
#' @param fixed [\code{logical(1L)}]\cr
#'  If \code{FALSE} (default), \code{pattern} is a regular expression and a fixed string otherwise.
#' @template reg
#' @export
#' @family debug
#' @return [\code{\link{data.table}}]. Matching job ids are stored in the column \dQuote{job.id}.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
grepLogs = function(ids = NULL, pattern, ignore.case = FALSE, fixed = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertString(pattern, min.chars = 1L)
  assertFlag(ignore.case)
  assertFlag(fixed)

  ids = convertIds(reg, ids, default = .findStarted(reg = reg))
  tab = inner_join(reg$status, ids)[, c("job.id", "job.hash"), with = FALSE]

  setorderv(tab, "job.hash")
  found = logical(nrow(tab))
  matches = character(nrow(tab))
  hash.before = ""
  matcher = if (fixed) stri_detect_fixed else stri_detect_regex

  for (i in seq_row(tab)) {
    if (hash.before != tab$job.hash[i]) {
      log = readLog(tab[i], impute = NA_character_, reg = reg)
      hash.before = tab$job.hash[i]
    }

    lines = extractLog(log, tab[i])
    if (!testScalarNA(lines)) {
      m = matcher(lines, pattern, case_insensitive = ignore.case)
      if (any(m)) {
        found[i] = TRUE
        matches[i] = stri_join(lines[m], collapse = "\n")
      }
    }
  }

  res = cbind(tab[found, "job.id", with = FALSE], data.table(matches = matches[found]))
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
  id = convertId(reg, id)
  lines = extractLog(readLog(id, reg = reg), id)
  log.file = file.path(tempdir(), sprintf("%i.log", id$job.id))
  writeLines(text = lines, con = log.file)
  file.show(log.file, delete.file = TRUE)
}

#' @export
#' @rdname showLog
getLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  id = convertId(reg, id)
  extractLog(readLog(id, reg = reg), id)
}
