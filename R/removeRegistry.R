#' @title Remove a Registry from the File System
#'
#' @description
#' All files will be erased from the file system, including all results.
#' If you wish to remove only intermediate files, use \code{\link{sweepRegistry}}.
#'
#' @param wait [\code{numeric(1)}]\cr
#'   Seconds to wait before proceeding. This is a safety measure to not
#'   accidentally remove your precious files. Set to 0 in
#'   non-interactive scripts to disable this precaution.
#' @template reg
#'
#' @return [\code{logical(1)}]: Success of \code{\link[base]{unlink}}.
#' @export
#' @family Registry
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' removeRegistry(0, tmp)
removeRegistry = function(wait = 5, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE, running.ok = FALSE)
  assertNumber(wait, lower = 0)

  if (wait > 0) {
    info("This deletes all files in '%s'. Proceeding in %g seconds ...", reg$file.dir, wait)
    Sys.sleep(wait)
  }

  if (identical(batchtools$default.registry$file.dir, reg$file.dir)) {
    info("Unsetting registry as default")
    setDefaultRegistry(NULL)
  }

  info("Recursively removing files in '%s' ...", reg$file.dir)
  unlink(reg$file.dir, recursive = TRUE) == 0L
}
