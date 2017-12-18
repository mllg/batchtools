#' @title Remove Experiments
#'
#' @description
#' Remove Experiments from an \code{\link{ExperimentRegistry}}.
#' This function automatically checks if any of the jobs to reset is either pending or running.
#' However, if the implemented heuristic fails, this can lead to inconsistencies in the data base.
#' Use with care while jobs are running.
#'
#' @templateVar ids.default none
#' @template ids
#' @template expreg
#' @return [\code{\link{data.table}}] of removed job ids, invisibly.
#' @export
#' @family Experiment
removeExperiments = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, class = "ExperimentRegistry", writeable = TRUE, running.ok = FALSE)
  ids = convertIds(reg, ids, default = noIds())

  info("Removing %i Experiments ...", nrow(ids))
  reg$status = reg$status[!ids]
  i = reg$defs[!reg$status, on = "def.id", which = TRUE]
  if (length(i) > 0L) {
    info("Removing %i job definitions ...", length(i))
    reg$defs = reg$defs[-i]
  }
  fns = getResultFiles(reg, ids)
  file.remove.safely(fns)

  sweepRegistry(reg)
  invisible(ids)
}
