runHook = function(obj, hook, ...) {
  UseMethod("runHook")
}

runHook.Registry = function(obj, hook, ...) {
  f = obj$cluster.functions$hooks[[hook]]
  if (is.null(f)) NULL else f(obj, ...)
}

runHook.JobCollection = function(obj, hook, ...) {
  f = obj$hooks[[hook]]
  if (is.null(f)) NULL else f(obj, ...)
}
