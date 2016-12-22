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
#' @importFrom stats runif predict
"_PACKAGE"

#' @title Deprecated function in the batchtools package
#' @rdname batchtools-deprecated
#' @name batchtools-deprecated
#' @description
#' The following functions have been deprecated:
#' \tabular{rl}{
#'   \code{chunkIds} \tab deprecated in favor of \code{\link{chunk}}, \code{\link{lpt}} and \code{\link{binpack}}\cr
#' }
NULL

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

.onAttach = function(libname, pkgname) { #nocov start
  if (batchtools$debug) {
    packageStartupMessage("Debug mode of batchtools enabled")
  }
}

.onUnload = function (libpath) {
  library.dynam.unload("batchtools", libpath) # nocov
}
