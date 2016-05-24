#' @description
#' For bug reports and feature requests please use the tracker:
#' \url{https://github.com/mllg/batchtools}.
#'
#' @section Package options:
#' \describe{
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
#' @importFrom utils head tail
"_PACKAGE"

batchtools = new.env(parent = emptyenv())
batchtools$hooks = c("pre.sync", "post.sync", "pre.do.collection", "post.do.collection", "pre.submit", "post.submit")
batchtools$remote.hooks = c("pre.do.collection", "post.do.collection")
job.id = def.id = pars = submitted = started = done = error = batch.id = status = problem = algorithm = repl = NULL

.onUnload <- function (libpath) {
  library.dynam.unload("batchtools", libpath) # nocov
}
