#' @description
#' For bug reports and feature requests please use the tracker:
#' \url{https://github.com/mllg/batchtools}.
#'
#' @section Package options:
#' \describe{
#'   \item{\code{batchtools.verbose}}{
#'     Verbosity. Set to \code{FALSE} to suppress info messages and progress bars.
#'   }
#'   \item{\code{batchtools.progress}}{
#'     Progress bars. Set to \code{FALSE} to disable them.
#'   }
#'   \item{\code{batchtools.timestamps}}{
#'     Add time stamps to log output. Set to \code{FALSE} to disable them.
#'   }
#' }
#' Furthermore, you may enable a debug mode using the \pkg{debugme} package by
#' setting the environment variable \dQuote{DEBUGME} to \dQuote{batchtools} before
#' loading \pkg{batchtools}.
#' @import utils
#' @import checkmate
#' @import stringi
#' @import data.table
#' @importFrom R6 R6Class
#' @importFrom digest digest
#' @importFrom brew brew
#' @importFrom progress progress_bar
#' @importFrom rappdirs user_config_dir site_config_dir
#' @importFrom stats runif predict pexp
#' @importFrom base64url base32_encode base32_decode
#' @importFrom withr with_dir with_seed local_options local_dir
"_PACKAGE"

batchtools = new.env(parent = emptyenv())
batchtools$debug = FALSE
batchtools$hooks = list(
  remote = c("pre.do.collection", "post.do.collection"),
  local  = c("pre.sync", "post.sync", "pre.submit.job", "post.submit.job", "pre.submit", "post.submit", "pre.kill", "post.kill")
)

batchtools$resources = list(
  per.job = c("walltime", "memory", "ncpus", "omp.threads", "blas.threads"),
  per.chunk = c("measure.memory", "chunks.as.arrayjobs", "pm.backend", "foreach.backend")
)

.onLoad = function(libname, pkgname) { # nocov start
  if (requireNamespace("debugme", quietly = TRUE) && "batchtools" %in% strsplit(Sys.getenv("DEBUGME"), ",", fixed = TRUE)[[1L]]) {
    debugme::debugme()
    batchtools$debug = TRUE
  }
  backports::import(pkgname, "...length")
  backports::import(pkgname, "hasName", force = TRUE)
} # nocov end

.onUnload = function (libpath) { # nocov start
  library.dynam.unload("batchtools", libpath)
} # nocov end
