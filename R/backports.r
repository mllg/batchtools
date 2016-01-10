if (getRversion() < "3.2.1") {
  lengths = function(x, use.names = TRUE) {
    vapply(x, length, FUN.VALUE = NA_integer_, USE.NAMES = use.names)
  }

  if (getRversion() < "3.2.0") {
    dir.exists = function(paths) {
      x = file.info(paths)$isdir
      !is.na(x) & x
    }

    forceAndCall = function(n, FUN, ...) {
      FUN = match.fun(FUN)
      do.call(FUN, list(...))
    }
  }
}
