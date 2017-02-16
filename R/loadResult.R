#' @title Load the Result of a Single Job
#'
#' @description
#' Loads the result of a single job.
#'
#' @template id
#' @template reg
#' @return [\code{ANY}]. The stored result.
#' @family Results
#' @export
loadResult = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  id = convertId(reg, id)
  if (nrow(.findDone(reg, id)) == 0L)
    stopf("Job with id %i not terminated", id$job.id)
  fn = getResultFiles(reg, id)
  return(readRDS(fn))
}
