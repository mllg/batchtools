#' @description
#' For bug reports and feature requests please use the tracker:
#' \url{https://github.com/mllg/batchtools}.
#'
#' @section Package options:
#' \describe{
#'   \item{\code{batchtools.progress}}{
#'     Progress bars. Set to \code{FALSE} to disable them.
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
#' @importFrom utils head tail getFromNamespace
"_PACKAGE"

batchtools = new.env(parent = emptyenv())
batchtools$hooks = data.table(
  name =   c("pre.sync", "post.sync", "pre.do.collection", "post.do.collection", "pre.submit", "post.submit"),
  remote = c(FALSE, FALSE, TRUE, TRUE, FALSE, FALSE)
)

.onUnload <- function (libpath) {
  library.dynam.unload("batchtools", libpath) # nocov
}
