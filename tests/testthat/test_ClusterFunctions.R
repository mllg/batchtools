context("clusterFunctions")

test_that("clusterFunctions constructor", {
  check = function(cf) {
    expect_is(cf, "ClusterFunctions")
    expect_set_equal(names(cf), c("name", "submitJob", "killJob", "listJobsQueued", "listJobsRunning", "store.job", "array.var", "hooks"))
    expect_output(print(cf), "ClusterFunctions for mode")
  }
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  check(reg$cluster.functions)
  check(makeClusterFunctionsInteractive())
  if (!testOS("windows"))
    check(makeClusterFunctionsSSH(workers = list(Worker$new(nodename = "localhost", ncpus = 1L))))
  check(makeClusterFunctionsSGE(template = "foo\n"))
  check(makeClusterFunctionsTorque(template = "foo\n"))
  check(makeClusterFunctionsSlurm(template = "foo\n"))
  check(makeClusterFunctionsOpenLava(template = "foo\n"))
  check(makeClusterFunctionsLSF(template = "foo\n"))
  check(makeClusterFunctionsTorque(system.file(file.path("templates", "torque_lido.tmpl"), package = "batchtools")))
  check(makeClusterFunctionsSlurm(system.file(file.path("templates", "slurm_dortmund.tmpl"), package = "batchtools")))
  check(makeClusterFunctionsDocker("image"))

  expect_error(makeClusterFunctionsLSF(), "No template")
})


test_that("submitJobResult", {
  x = makeSubmitJobResult(0, 99)
  expect_is(x, "SubmitJobResult")
  expect_identical(x$status, 0L)
  expect_identical(x$batch.id, 99)
  expect_identical(x$msg, "OK")

  x = makeSubmitJobResult(1, 99)
  expect_is(x, "SubmitJobResult")
  expect_identical(x$msg, "TEMPERROR")

  x = makeSubmitJobResult(101, 99)
  expect_is(x, "SubmitJobResult")
  expect_identical(x$msg, "ERROR")

  expect_output(print(x), "submission result")

  x = cfHandleUnknownSubmitError(cmd = "ls", exit.code = 42L, output = "answer to life")
  expect_is(x, "SubmitJobResult")
  expect_true(all(stri_detect_fixed(x$msg, c("ls", "42", "answer to life"))))
})

test_that("brew", {
  fn = tempfile()
  lines = c("####", " ", "!!!", "foo=<%= job.hash %>")
  writeLines(lines, fn)

  res = stri_split_fixed(cfReadBrewTemplate(fn), "\n")[[1]]
  assertCharacter(res, len = 3)
  expect_equal(sum(stri_detect_fixed(res, "job.hash")), 1)

  res = stri_split_fixed(cfReadBrewTemplate(fn, comment.string = "###"), "\n")[[1]]
  assertCharacter(res, len = 2)
  expect_equal(sum(stri_detect_fixed(res, "job.hash")), 1)

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  ids = batchMap(identity, 1:2, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  text = cfReadBrewTemplate(fn, comment.string = "###")

  fn = cfBrewTemplate(text = text, jc = jc, reg = reg)
  brewed = readLines(fn)
  expect_equal(brewed[1], "!!!")
  expect_equal(brewed[2], sprintf("foo=%s", jc$job.hash))
})

test_that("Export of environment variable DEBUGME", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(function(i) Sys.getenv("DEBUGME"), i = 1, reg = reg)

  prev = Sys.getenv("DEBUGME")
  on.exit(Sys.setenv(DEBUGME = prev))
  Sys.setenv(DEBUGME = "grepme")
  submitAndWait(reg, 1)

  res = loadResult(1, reg = reg)
  expect_string(res, min.chars = 1, fixed = "grepme")
})
