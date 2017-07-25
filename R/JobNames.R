#' @title Set Custom Job Names
#'
#' @description
#' Set custom names for jobs. These are passed to the template as
#' \sQuote{job.name}.
#'
#' @templateVar ids.default none
#' @template ids
#' @param names [\code{character}]\cr
#'  Character vector of the same length as provided ids.
#' @template reg
#' @return Nothing.
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(identity, 1:10, reg = tmp)
#' setJobNames(ids, letters[1:nrow(ids)], reg = tmp)
setJobNames = function(ids, names, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids)
  assertCharacter(names, min.chars = 1L, len = nrow(ids), pattern = "^[[:alnum:]_.-]+$")

  reg$status[ids, job.names := names]
  saveRegistry(reg)
}
