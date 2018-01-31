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
#' @return [\code{character(1)}]: Path of the deleted file directory.
#' @export
#' @family Registry
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
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
  fs::dir_delete(reg$file.dir)
}
