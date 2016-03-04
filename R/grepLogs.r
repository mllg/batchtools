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
  assertRegistry(reg)
  ids = asJobIds(reg, ids, default = .findSubmitted(reg = reg))
  assertString(pattern)
  if (!nzchar(pattern))
    return(ids)

  found = lapply(ids$job.id, function(id) {
    lines = readLog(id, impute = NA_character_, reg = reg)
    !testScalarNA(lines) && any(stri_detect_regex(lines, pattern))
  })

  ids[found, ]
}
