#' @title Grep Log Files for a Pattern
#'
#' @description
#' Greps through log files and reports jobs with matches.
#' @templateVar ids.default findSubmitted
#' @template ids
#' @param pattern [\code{character(1L)}]\cr
#'  Regular expression.
#' @template reg
#' @export
#' @family debug
#' @return [\code{data.table}]. Matching job ids are stored in the column \dQuote{job.id}.
grepLogs = function(ids = NULL, pattern, reg = getDefaultRegistry()) {
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
  ids = filter(reg$status, ids %??% .findSubmitted(reg = reg))[, c("job.id", "job.hash"), with = FALSE]
  if (!nzchar(pattern))
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

  setkeyv(cbind(ids[found, "job.id", with = FALSE], data.table(matches = matches[found])), "job.id")
}
