#' @title Remove All Jobs
#'
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
  sweepRegistry(reg = reg)
}
