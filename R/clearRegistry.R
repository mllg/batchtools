#' @title Remove All Jobs
#' @description
#' Removes all jobs from a registry and calls \code{\link{sweepRegistry}}.
#'
#' @template reg
#' @family Registry
#' @export
clearRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, running.ok = FALSE)
  info("Removing %i jobs ...", nrow(reg$status))
  reg$status = reg$status[FALSE]
  reg$defs = reg$defs[FALSE]
  reg$resources = reg$resources[FALSE]
  user.fun = fp(reg$file.dir, "user.function.rds")
  if (file.exists(user.fun)) {
    info("Removing user function ...")
    file.remove.safely(user.fun)
  }
  sweepRegistry(reg = reg)
}
