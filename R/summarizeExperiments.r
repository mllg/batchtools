#' @title Quick Summary over Experiments
#'
#' @description
#' Returns a frequency table of defined experiments.
#'
#' @templateVar ids.default all
#' @template ids
#' @param by [\code{character}]\cr
#'   Split the resulting table by these columns of \code{\link{getJobDefs}}.
#' @template reg
#' @return [\code{\link[base]{data.table}}] of frequencies in the respective groups.
summarizeExperiments = function(ids = NULL, by = c("problem", "algorithm"), reg = getDefaultRegistry()) {
  pars = !setequal(by, c("problem", "algorithm"))
  getJobDefs(ids = ids, pars.as.cols = pars, reg = reg)[, list(.count = .N), by = by]
}
