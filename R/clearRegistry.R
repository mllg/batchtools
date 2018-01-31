#' @title Remove All Jobs
#' @description
#' Removes all jobs from a registry and calls \code{\link{sweepRegistry}}.
#'
#' @template reg
#' @family Registry
#' @export
clearRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE, running.ok = FALSE)
  info("Removing %i jobs ...", nrow(reg$status))
  reg$status = reg$status[FALSE]
  reg$defs = reg$defs[FALSE]
  reg$resources = reg$resources[FALSE]
  user.fun = fs::path(reg$file.dir, "user.function.rds")
  if (fs::file_exists(user.fun)) {
    info("Removing user function ...")
    file_remove(user.fun)
  }
  sweepRegistry(reg = reg)
}
