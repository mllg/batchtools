test_that("clusterFunctions constructor", {
  check = function(cf) {
    expect_is(cf, "ClusterFunctions")
    expect_set_equal(names(cf), c("name", "submitJob", "killJob", "listJobsQueued", "listJobsRunning",
        "store.job.collection", "store.job.files", "array.var", "scheduler.latency", "fs.latency", "hooks"))
    expect_output(print(cf), "ClusterFunctions for mode")
  }
  reg = makeTestRegistry()
  check(reg$cluster.functions)
  fn = fs::path(fs::path_temp(), "dummy.tmpl")
  writeLines("foo", fn)
  check(makeClusterFunctionsInteractive())
  check(makeClusterFunctionsSGE(template = fn))
  check(makeClusterFunctionsTORQUE(template = fn))
  check(makeClusterFunctionsSlurm(template = fn))
  check(makeClusterFunctionsOpenLava(template = fn))
  check(makeClusterFunctionsLSF(template = fn))
  check(makeClusterFunctionsTORQUE("torque-lido"))
  check(makeClusterFunctionsSlurm("slurm-dortmund"))
  check(makeClusterFunctionsDocker("image"))
  expect_error(makeClusterFunctionsLSF(), "point to a readable template file")

  skip_on_os(c("windows", "solaris")) # system2 is broken on solaris
    check(makeClusterFunctionsSSH(workers = list(Worker$new(nodename = "localhost", ncpus = 1L))))
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
  fn = fs::file_temp()
  lines = c("####", " ", "!!!", "foo=<%= job.hash %>")
  writeLines(lines, fn)

  res = stri_split_fixed(cfReadBrewTemplate(fn), "\n")[[1]]
  assertCharacter(res, len = 3)
  expect_equal(sum(stri_detect_fixed(res, "job.hash")), 1)

  res = stri_split_fixed(cfReadBrewTemplate(fn, comment.string = "###"), "\n")[[1]]
  assertCharacter(res, len = 2)
  expect_equal(sum(stri_detect_fixed(res, "job.hash")), 1)

  reg = makeTestRegistry()
  ids = batchMap(identity, 1:2, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  text = cfReadBrewTemplate(fn, comment.string = "###")

  fn = cfBrewTemplate(text = text, jc = jc, reg = reg)
  brewed = readLines(fn)
  expect_equal(brewed[1], "!!!")
  expect_equal(brewed[2], sprintf("foo=%s", jc$job.hash))
  fs::file_delete(fn)
})

test_that("Special chars in directory names", {
  reg = makeTestRegistry()
  base.dir = fs::file_temp(pattern = "test", tmp_dir = fs::path_dir(reg$file.dir))
  fs::dir_create(base.dir)

  file.dir = fs::path(base.dir, "test#some_frequently-used chars")
  reg = makeTestRegistry(file.dir = file.dir)
  batchMap(identity, 1:2, reg = reg)
  submitAndWait(reg = reg)
  Sys.sleep(0.2)
  expect_equal(reduceResultsList(reg = reg), list(1L, 2L))
  expect_equal(testJob(1, external = FALSE, reg = reg), 1L)
})

test_that("Export of environment variable DEBUGME", {
  reg = makeTestRegistry()
  if (reg$cluster.functions$name == "Socket")
    skip("Environment variables not exported for CF socket")
  batchMap(function(i) Sys.getenv("DEBUGME"), i = 1, reg = reg)

  withr::local_envvar(c("DEBUGME" = "grepme"))
  submitAndWait(reg, 1)

  res = loadResult(1, reg = reg)
  expect_string(res, min.chars = 1, fixed = "grepme")
})

test_that("findTemplateFile", {
  d = fs::path_temp()
  fn = fs::path(d, "batchtools.slurm.tmpl")
  fs::file_create(fn)
  withr::with_envvar(list(R_BATCHTOOLS_SEARCH_PATH = d),
    expect_equal(findTemplateFile("slurm"), fs::path_abs(fn))
  )
  fs::file_delete(fn)
})
