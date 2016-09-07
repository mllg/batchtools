#' @param ncpus [\code{integer(1)}]\cr
#'   Number of VPUs of worker.
#'   Default is to use all cores. The total number of cores "available" can be set via the option \code{mc.cores}
#'   and defaults to the heuristic implemented in \code{\link[parallel]{detectCores}}.
