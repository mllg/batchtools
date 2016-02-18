if (getRversion() < "3.2.0") {
  forceAndCall = function(n, FUN, ...) {
    FUN = match.fun(FUN)
    do.call(FUN, list(...))
  }
}
