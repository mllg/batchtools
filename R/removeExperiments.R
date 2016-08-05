#' @title Remove Experiments
#'
#' @description
#' Remove Experiments from an \code{\link{ExperimentRegistry}}.
#' This function automatically checks if any of the jobs to reset is either pending or running.
#' However, if the implemented heuristic fails, this can lead to inconsistencies in the data base.
#' Use with care.
#'
#' @templateVar ids.default none
#' @template ids
#' @template expreg
#' @return [\code{\link{data.table}}] of removed job ids.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @export
#' @family Experiment
removeExperiments = function(ids = NULL, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE, running.ok = FALSE)
  ids = convertIds(reg, ids, default = copy(noids))

  info("Removing %i Experiments", nrow(ids))
  reg$status = reg$status[!ids]

  i = which(reg$defs$def.id %nin% reg$status$def.id)
  if (length(i) > 0L) {
    info("Cleaning up %i job definitions", length(i))
    reg$defs = reg$defs[-i]
  }

  sweepRegistry(reg)
  return(ids)
}
