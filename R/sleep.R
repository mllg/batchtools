default.sleep = function(i) {
   5 + 115 * pexp(i - 1, rate = 0.01)
}

getSleepFunction = function(sleep) {
  assert(checkNumber(sleep, lower = 0), checkFunction(sleep, args = "i"))
  if (is.numeric(sleep))
    function(i) Sys.sleep(sleep)
  else
    function(i) Sys.sleep(sleep(i))
}
