#' @title Set and Retrieve Job Names
#' @name JobNames
#'
#' @description
#' Set custom names for jobs. These are passed to the template as \sQuote{job.name}.
#' If no custom name is set (or any of the job names of the chunk is missing),
#' the job hash is used as job name.
#' Individual job names can be accessed via \code{jobs$job.name}.
#'
#' @templateVar ids.default all
#' @template ids
#' @param names [\code{character}]\cr
#'  Character vector of the same length as provided ids.
#' @template reg
#' @return \code{setJobNames} returns \code{NULL} invisibly, \code{getJobTable}
#'  returns a \code{data.table} with columns \code{job.id} and \code{job.name}.
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(identity, 1:10, reg = tmp)
#' setJobNames(ids, letters[1:nrow(ids)], reg = tmp)
#' getJobNames(reg = tmp)
setJobNames = function(ids = NULL, names, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids, default = noIds())
  assertCharacter(names, min.chars = 1L, len = nrow(ids))

  reg$status[ids, "job.name" := names]
  saveRegistry(reg)
  invisible(NULL)
}

#' @export
#' @rdname JobNames
getJobNames = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = allIds(reg))
  reg$status[ids, c("job.id", "job.name")]
}
