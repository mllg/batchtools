#' @title Retrieve Error Messages
#'
#' @description
#' Extracts error messages from the internal data base and returns them in a compact table.
#' For further investigation, see \code{\link{showLog}}.
#' @templateVar ids.default findErrors
#' @template ids
#' @param missing.as.error [\code{logical(1)}]\cr
#'   Treat missing results as errors? If \code{TRUE}, an error message is imputed for jobs
#'   which are not terminated. Default is \code{FALSE}
#' @template reg
#' @return [\code{\link{data.table}}]. Table with columns \dQuote{job.id}, \dQuote{terminated} (logical),
#'   \dQuote{error} (logical) and \dQuote{message} (string).
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @family debug
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' fun = function(i) if (i == 3) stop(i) else i
#' ids = batchMap(fun, i = 1:5, reg = reg)
#' submitJobs(1:4, reg = reg)
#' waitForJobs(1:4, reg = reg)
#' getErrorMessages(ids, reg = reg)
#' getErrorMessages(ids, missing.as.error = TRUE, reg = reg)
getErrorMessages = function(ids = NULL, missing.as.error = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertFlag(missing.as.error)
  job.id = done = error = NULL
  tab = filter(reg$status, ids %??% .findErrors(reg = reg))[, list(job.id, terminated = !is.na(done), error = !is.na(error), message = error)]

  if (missing.as.error) {
    tab[!tab$terminated, c("error", "message") := list(TRUE, "Not terminated")]
  }

  return(tab[])
}
