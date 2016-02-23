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
#' @importFrom progress progress_bar
#' @importFrom stats runif
#' @importFrom utils capture.output head tail
"_PACKAGE"

batchtools = new.env(parent = emptyenv())

no.ids = data.table(job.id = integer(0L), key = "job.id")

# global variables frequently used with data.tables
job.id = def.id = pars = NULL
submitted = started = done = error = NULL
batch.id = status = NULL
problem = algorithm = repl = NULL
