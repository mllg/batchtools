context("doJobCollection")

test_that("doJobCollection handles bulky log output", {
  N = 1e5
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(N) print(paste(rep("a", N), collapse = ""))
  batchMap(fun, N, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  fn = tempfile()
  doJobCollection(jc, con = fn)
  lines = readLines(fn)
  expect_true(any(nchar(lines) >= N))
})

test_that("doJobCollection truncates error messages", {
  N = 5000 # R truncates stop() at 2^13 chars
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(N) stop(paste(rep("a", N), collapse = ""))
  batchMap(fun, N, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  fn = tempfile()
  doJobCollection(jc, con = fn)
  syncRegistry(reg = reg)
  msg = getErrorMessages(reg = reg)$message
  expect_true(stri_endswith_fixed(msg, " [truncated]"))
})
