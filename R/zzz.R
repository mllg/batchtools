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
#' Furthermore, you may enable a debug mode using the \pkg{debugme} package by
#' setting the environment variable \dQuote{DEBUGME} to \dQuote{batchtools} before
#' loading \pkg{batchtools}.
#' @import utils
#' @import backports
#' @import checkmate
#' @import data.table
#' @import stringi
#' @importFrom R6 R6Class
#' @importFrom digest digest
#' @importFrom brew brew
#' @importFrom progress progress_bar
#' @importFrom rappdirs user_config_dir
#' @importFrom stats runif
"_PACKAGE"

batchtools = new.env(parent = emptyenv())
batchtools$debug = FALSE
batchtools$hooks = data.table(
  name =   c("pre.sync", "post.sync", "pre.do.collection", "post.do.collection", "pre.submit", "post.submit"),
  remote = c(FALSE, FALSE, TRUE, TRUE, FALSE, FALSE)
)

.onLoad = function(libname, pkgname) { # nocov start
  if (requireNamespace("debugme", quietly = TRUE) && "batchtools" %in% strsplit(Sys.getenv("DEBUGME"), ",", fixed = TRUE)[[1L]]) {
    debugme::debugme()
    batchtools$debug = TRUE
  }
} # nocov end

.onUnload = function (libpath) {
  library.dynam.unload("batchtools", libpath) # nocov
}
