#' @param ids [\code{\link[base]{data.frame}} or \code{integer}]\cr
#'   A \code{\link[base]{data.frame}} (or \code{\link[data.table]{data.table}})
#'   with a column named \dQuote{job.id}.
#'   Alternatively, you may also pass a vector of integerish job ids.
#'   If not set, defaults to <%= switch(ids.default, "all" = "all jobs", "none" = "no job", sprintf("the return value of \\code{\\link{%s}}", ids.default)) %>.
#'   Invalid ids are ignored.
