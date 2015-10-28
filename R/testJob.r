#' @title Run Jobs Interactively
#'
#' @description
#' Starts a single job in the current R session.
#' Debugging tools like \code{\link[base]{traceback}} should work.
#'
#' @template id
#' @template reg
#' @return [ANY]. Returns the result of the job if successful.
#' @export
#' @family debug
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(function(x) if (x == 2) stop(x) else x, 1:2, reg = reg)
#' testJob(1, reg = reg)
#' \dontrun{
#'  testJob(2, reg = reg)
#' }
testJob = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  id = asIds(reg, id, n = 1L)
  cache = Cache(reg$file.dir)
  jd = makeJobDescription(id, reg = reg)
  execJob(jd = jd, i = id, cache = cache)
}
