#' @title Get and Set the Default Registry
#' @description
#' \code{getDefaultRegistry} returns the registry currently set as default (or
#' stops with an exception if none is set). \code{setDefaultRegistry} sets
#' a registry as default.
#'
#' @template reg
#' @family Registry
#' @export
getDefaultRegistry = function() {
  if (is.null(batchtools$default.registry))
    stop("No default registry defined")
  batchtools$default.registry
}

#' @export
#' @rdname getDefaultRegistry
setDefaultRegistry = function(reg) {
  if (!is.null(reg))
    assertRegistry(reg)
  batchtools$default.registry = reg
}
