#' @useDynLib batchtools fill_gaps
readLog = function(id, missing.as.empty = FALSE, reg = getDefaultRegistry()) {
  log.file = getLogFiles(reg, id)
  if (is.na(log.file) || !file.exists(log.file)) {
    if (missing.as.empty)
      return(data.table(job.id = integer(0L), lines = character(0L)))
    stopf("Log file for job with id %i not available", id$job.id)
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
#' Crawls through log files and reports jobs with lines matching the \code{pattern}.
#' See \code{\link{showLog}} for an example.
#'
#' @templateVar ids.default findStarted
#' @template ids
#' @param pattern [\code{character(1L)}]\cr
#'  Regular expression or string (see \code{fixed}).
#' @param ignore.case [\code{logical(1L)}]\cr
#'  If \code{TRUE} the match will be performed case insensitively.
#' @param fixed [\code{logical(1L)}]\cr
#'  If \code{FALSE} (default), \code{pattern} is a regular expression and a fixed string otherwise.
#' @template reg
#' @export
#' @family debug
#' @return [\code{\link{data.table}}] with columns \dQuote{job.id} and \dQuote{message}.
grepLogs = function(ids = NULL, pattern, ignore.case = FALSE, fixed = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertString(pattern, min.chars = 1L)
  assertFlag(ignore.case)
  assertFlag(fixed)
  job.id = job.hash = log.file = matches = NULL

  ids = convertIds(reg, ids, default = .findStarted(reg = reg))
  tab = filter(reg$status[!is.na(job.hash)], ids)[, list(job.id = job.id, hash = sprintf("%s-%s", job.hash, log.file))]
  if (nrow(tab) == 0L)
    return(data.table(job.id = integer(0L), matches = character(0L)))

  setorderv(tab, "hash")
  res = data.table(job.id = tab$job.id, matches = NA_character_)
  hash.before = ""
  matcher = if (fixed) stri_detect_fixed else stri_detect_regex

  for (i in seq_row(tab)) {
    if (hash.before != tab$hash[i]) {
      log = readLog(tab[i], missing.as.empty = TRUE, reg = reg)
      hash.before = tab$hash[i]
    }

    if (nrow(log) > 0L) {
      lines = extractLog(log, tab[i])
      m = matcher(lines, pattern, case_insensitive = ignore.case)
      if (any(m))
        set(res, i, "matches", stri_flatten(lines[m], "\n"))
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
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # Create some dummy jobs
#' fun = function(i) {
#'   if (i == 3) stop(i)
#'   if (i %% 2 == 1) warning("That's odd.")
#' }
#' ids = batchMap(fun, i = 1:5, reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' getStatus(reg = tmp)
#'
#' writeLines(getLog(ids[1], reg = tmp))
#' \dontrun{
#' showLog(ids[1], reg = tmp)
#' }
#'
#' grepLogs(pattern = "warning", ignore.case = TRUE, reg = tmp)
showLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  id = convertId(reg, id)
  lines = extractLog(readLog(id, reg = reg), id)
  log.file = fp(tempdir(), sprintf("%i.log", id$job.id))
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
