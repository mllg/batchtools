#' @title Quick Summary over Experiments
#'
#' @description
#' Returns a frequency table of defined experiments.
#' See \code{\link{ExperimentRegistry}} for an example.
#'
#' @templateVar ids.default all
#' @template ids
#' @param by [\code{character}]\cr
#'   Split the resulting table by columns of \code{\link{getJobPars}}.
#' @template expreg
#' @return [\code{\link{data.table}}] of frequencies.
#' @export
#' @family Experiment
summarizeExperiments = function(ids = NULL, by = c("problem", "algorithm"), reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  assertCharacter(by, any.missing = FALSE, min.chars = 1L, min.len = 1L, unique = TRUE)
  tab = getJobPars(ids = ids, reg = reg)
  if (!setequal(by, c("problem", "algorithm")))
    tab = flatten(tab)
  tab[, list(.count = .N), by = by]
}
