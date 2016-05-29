#' @title Quick Summary over Experiments
#'
#' @description
#' Returns a frequency table of defined experiments.
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
  pars = !setequal(by, c("problem", "algorithm"))
  getJobDefs(ids = ids, flatten = pars, reg = reg)[, list(.count = .N), by = by]
}
