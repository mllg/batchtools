#' The batchtools Package
#'
#' @name batchtools
#' @docType package
#' @description
#' For bug reports and feature requests please use the tracker:
#' \url{https://github.com/mllg/batchtools}.
#'
#' @section Package options:
#' \describe{
#'   \item{\code{batchtools.compress}}{
#'     Compression to use in \code{\link[base]{saveRDS}} which is used to write the \code{\link{Registry}},
#'     \code{\link{JobCollection}} jobs and result files. For very slow file systems, set to \dQuote{xz}
#'     to achieve less IO at the expense of more CPU cycles.
#'     Defaults to \code{TRUE} (\dQuote{gzip} compression).
#'   }
#'   \item{\code{batchtools.conf.file}}{
#'     Default path to file which will be sourced while creating a \code{\link{Registry}}.
#'     Can be used to set system-wide \code{\link{ClusterFunctions}} or default resources.
#'   }
#'   \item{\code{batchtools.ncpus}}{
#'     Numbers of cores to use on the master node via \code{\link[parallel]{mclapply}}.
#'     Defaults to one core, i.e. sequential execution.
#'   }
#'   \item{\code{batchtools.temp.dir}}{
#'     Temporary directory to use in \code{\link{makeTempRegistry}}.
#'     On some cluster systems you might want to set this to a directory which is shared between nodes.
#'     Default is the system temp directory as returned by \code{\link[base]{tempdir}}.
#'   }
#'   \item{\code{batchtools.verbose}}{
#'     Verbosity. Set to \code{FALSE} to suppress info messages and progress bars.
#'   }
#' }
#' @import checkmate
#' @import data.table
#' @import stringi
#' @importFrom progress progress_bar
#' @importFrom stats runif
#' @importFrom utils capture.output head tail
NULL

batchtools = new.env(parent = emptyenv())
