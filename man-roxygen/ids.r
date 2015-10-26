#' @param ids [\code{\link[base]{data.frame}} or \code{integer}]\cr
#'   A \code{\link[base]{data.frame}} (or \code{\link[data.table]{data.table}})
#'   with a column named \dQuote{job.id}.
#'   This is usually the result of a \code{\link{findJobs}} operation, but you may also
#'   pass any other table here, e.g. the return of \code{\link{getJobInfo}}.
#'   Alternatively, you may also pass a vector of integerish job ids.
#'   If not set, defaults to <%= ifelse(ids.default == "all", "all jobs", sprintf("result of \\code{\\link{%s}}", ids.default)) %>.
