#' @useDynLib batchtools fill_gaps
readLog = function(id, missing.as.empty = FALSE, reg = getDefaultRegistry()) {
  tab = reg$status[id, c("job.id", "done", "job.hash"), with = FALSE, nomatch = NA]
  log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", tab$job.hash))

  if (is.na(tab$job.hash) || !file.exists(log.file)) {
    if (missing.as.empty)
      return(data.table(job.id = integer(0L), lines = character(0L)))
    stopf("Log file for job with id %i not available", tab$job.id)
  }

  lines = readLines(log.file)
  if (length(lines) > 0L) {
    job.id = as.integer(stri_match_last_regex(lines, c("\\[batchtools job\\.id=([0-9]+)\\]$"))[, 2L])
    job.id = .Call(fill_gaps, job.id)
  } else {
    job.id = integer(0L)
  }

  setkeyv(data.table(job.id = job.id, lines = lines), "job.id", physical = FALSE)
}

extractLog = function(log, id) {
  job.id = NULL
  log[is.na(job.id) | job.id %in% id$job.id]$lines
}

#' @title Grep Log Files for a Pattern
#'
#' @description
#' Crawls through log files and reports jobs with where lines matches the \code{pattern}.
#'
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
  job.hash = matches = NULL

  ids = convertIds(reg, ids)
  tab = filter(reg$status[!is.na(job.hash)], ids, c("job.id", "job.hash"))
  if (nrow(tab) == 0L)
    return(data.table(job.id = integer(0L), matches = character(0L)))

  setorderv(tab, "job.hash")
  res = data.table(job.id = tab$job.id, matches = NA_character_)
  hash.before = ""
  matcher = if (fixed) stri_detect_fixed else stri_detect_regex

  for (i in seq_row(tab)) {
    if (hash.before != tab$job.hash[i]) {
      log = readLog(tab[i], missing.as.empty = TRUE, reg = reg)
      hash.before = tab$job.hash[i]
    }

    if (nrow(log) > 0L) {
      lines = extractLog(log, tab[i])
      m = matcher(lines, pattern, case_insensitive = ignore.case)
      if (any(m))
        set(res, i, "matches", stri_join(lines[m], collapse = "\n"))
    }
  }

  setkeyv(res[!is.na(matches)], "job.id")[]
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
