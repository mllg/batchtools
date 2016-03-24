runHook = function(reg, hook, ...) {
  f = reg$cluster.functions$hook[[hook]]
  if (!is.null(f)) f(reg, ...) else invisible(NULL)
}
