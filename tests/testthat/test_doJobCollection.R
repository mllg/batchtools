context("doJobCollection")

test_that("doJobCollection handles bulky log output", {
  N = 1e5
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(N) print(paste(rep("a", N), collapse = ""))
  batchMap(fun, N, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  fn = tempfile()
  doJobCollection(jc, output = fn)
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
  doJobCollection(jc, output = fn)
  syncRegistry(reg = reg)
  msg = getErrorMessages(reg = reg)$message
  expect_true(stri_endswith_fixed(msg, " [truncated]"))
})

test_that("doJobCollection does not swallow warning messages", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsInteractive(external = TRUE)
  fun = function(x) warning("GREPME")
  batchMap(fun, 1, reg = reg)
  submitAndWait(reg, 1)
  expect_data_table(grepLogs(pattern = "GREPME", reg = reg), nrow = 1)
})

test_that("doJobCollection signals slave errors", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fn = tempfile(fileext = ".R", tmpdir = reg$temp.dir)
  reg$source = fn
  saveRegistry(reg)
  assign("y_on_master", 2, envir = .GlobalEnv)
  writeLines("x <- y_on_master", fn)
  rm(y_on_master, envir = .GlobalEnv)

  expect_error(withr::with_dir(reg$work.dir, loadRegistryDependencies(reg, must.work = TRUE)), "y_on_master")
  batchMap(identity, 1, reg = reg)
  submitAndWait(reg, 1)
  expect_data_table(findErrors(reg = reg), nrow = 1)
  expect_string(getErrorMessages(reg = reg)$message, fixed = "y_on_master")
  file.remove(fn)
})
