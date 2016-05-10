context("doJobCollection")

test_that("doJobCollection handles bulky log output", {
  N = 1e5
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(N) print(paste(rep("a", N), collapse = ""))
  batchMap(fun, N, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  fn = tempfile()
  expect_true(doJobCollection(jc, con = fn))
  lines = readLines(fn)
  expect_true(any(nchar(lines) >= N))
})
