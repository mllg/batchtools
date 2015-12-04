#' @title Quick Summary over Experiments
#'
#' @description
#' Returns a frequency table of defined experiments.
#'
#' @templateVar ids.default all
#' @template ids
#' @param by [\code{character}]\cr
#'   Split the resulting table by columns of \code{\link{getJobPars}}.
#' @template reg
#' @return [\code{named list}] with elements \dQuote{table}
#' (\code{\link[data.table]{data.table}}] of frequencies), \dQuote{problems}
#' (\code{character} of problem names) and \dQuote{algorithms}
#' (\code{character} of algorithm names).
#' @export
summarizeExperiments = function(ids = NULL, by = c("problem", "algorithm"), reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  pars = !setequal(by, c("problem", "algorithm"))
  setClasses(list(
    problems = levels(reg$defs$problem),
    algorithms = levels(reg$defs$algorithm),
    table = getJobDefs(ids = ids, pars.as.cols = pars, reg = reg)[, list(.count = .N), by = by]
  ), "ExperimentSummary")
}

#' @export
print.ExperimentSummary = function(x, ...) {
  print(x$table)
}
