#' @title Load the Result of a Single Job
#'
#' @description
#' Loads the result of a single job.
#'
#' @template id
#' @template missing.val
#' @template reg
#' @return [\code{ANY}]. The saved result or \code{missing.val} if result file
#'   is not found.
#' @family Results
#' @export
loadResult = function(id, missing.val, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  id = convertId(reg, id)
  if (missing(missing.val) && nrow(.findDone(reg, id)) == 0L)
    stop("Job with not terminated")
  .loadResult(reg$file.dir, id$job.id, missing.val)
}

.loadResult = function(file.dir, id, missing.val) {
  fn = file.path(file.dir, "results", sprintf("%i.rds", id))
  if (!file.exists(fn)) {
    if (missing(missing.val))
      stopf("Result for job with id=%i not found in %s", id, fn)
    return(missing.val)
  }
  return(readRDS(fn))
}
