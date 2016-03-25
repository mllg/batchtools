#' @description
#' For bug reports and feature requests please use the tracker:
#' \url{https://github.com/mllg/batchtools}.
#'
#' @section Package options:
#' \describe{
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
#' @import backports
#' @importFrom R6 R6Class
#' @importFrom progress progress_bar
#' @importFrom stats runif
#' @importFrom utils capture.output head tail
"_PACKAGE"

batchtools = new.env(parent = emptyenv())
job.id = def.id = pars = NULL
submitted = started = done = error = NULL
batch.id = status = NULL
problem = algorithm = repl = NULL
