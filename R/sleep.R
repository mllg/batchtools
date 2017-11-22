getSleepFunction = function(reg, sleep) {
  if (is.null(sleep)) {
    if (is.null(reg$sleep))
      return(function(i) { Sys.sleep(5 + 115 * pexp(i - 1, rate = 0.01)) })
    sleep = reg$sleep
  }

  if (is.numeric(sleep)) {
    assertNumber(sleep, lower = 0)
    return(function(i) Sys.sleep(sleep))
  }

  if (is.function(sleep)) {
    return(function(i) Sys.sleep(sleep(i)))
  }

  stop("Argument 'sleep' must be either a numeric value or function(i)")
}
