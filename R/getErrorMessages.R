#' @title Retrieve Error Messages
#'
#' @description
#' Extracts error messages from the internal data base and returns them in a table.
#'
#' @templateVar ids.default findErrors
#' @template ids
#' @param missing.as.error [\code{logical(1)}]\cr
#'   Treat missing results as errors? If \code{TRUE}, the error message \dQuote{[not terminated]} is imputed
#'   for jobs which have not terminated. Default is \code{FALSE}
#' @template reg
#' @return [\code{\link{data.table}}] with columns \dQuote{job.id}, \dQuote{terminated} (logical),
#'   \dQuote{error} (logical) and \dQuote{message} (string).
#' @family debug
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' fun = function(i) if (i == 3) stop(i) else i
#' ids = batchMap(fun, i = 1:5, reg = tmp)
#' submitJobs(1:4, reg = tmp)
#' waitForJobs(1:4, reg = tmp)
#' getErrorMessages(ids, reg = tmp)
#' getErrorMessages(ids, missing.as.error = TRUE, reg = tmp)
getErrorMessages = function(ids = NULL, missing.as.error = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  assertFlag(missing.as.error)
  ids = convertIds(reg, ids, default = .findErrors(reg = reg))

  job.id = done = error = NULL
  tab = reg$status[ids, list(job.id, terminated = !is.na(done), error = !is.na(error), message = error)]

  if (missing.as.error)
    tab[!tab$terminated, c("error", "message") := list(TRUE, "[not terminated]")]
  tab[]
}
