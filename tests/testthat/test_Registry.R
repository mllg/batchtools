context("Registry")

test_that("makeRegistry", {
  reg = makeTestRegistry()
  expect_is(reg, "Registry")
  expect_true(is.environment(reg))
  expect_directory_exists(reg$file.dir, access = "rw")
  expect_directory_exists(reg$work.dir, access = "r")
  expect_directory_exists(fs::path(reg$file.dir, c("jobs", "results", "updates", "logs")))
  expect_file_exists(fs::path(reg$file.dir, "registry.rds"))
  expect_character(reg$packages, any.missing = FALSE)
  expect_character(reg$namespaces, any.missing = FALSE)
  expect_int(reg$seed, na.ok = FALSE)
  expect_true(reg$writeable)
  expect_is(reg$cluster.functions, "ClusterFunctions")
  expect_list(reg$default.resources, names = "strict")
  checkTables(reg, any.missing = FALSE, nrows = 0L)

  reg = makeTestRegistry(packages = "checkmate", seed = 123)
  expect_equal(reg$packages, "checkmate")
  expect_int(reg$seed)
  expect_identical(reg$seed, 123L)

  expect_output(print(reg), "Registry")
})

test_that("reading conf file", {
  fn = fs::file_temp("conf")
  writeLines(con = fn, "default.resources = list(walltime = 42)")
  reg = makeTestRegistry(conf.file = fn)
  expect_identical(reg$default.resources, list(walltime = 42))
  fs::file_delete(fn)
})

test_that("make.default does work", {
  prev = batchtools$default.registry

  setDefaultRegistry(NULL)
  expect_error(getDefaultRegistry(), "No default")

  reg = makeTestRegistry(make.default = TRUE, seed = 123)
  expect_equal(reg$seed, 123L)
  reg = makeTestRegistry(make.default = FALSE, seed = 124)
  expect_equal(reg$seed, 124L)
  expect_class(getDefaultRegistry(), "Registry")
  expect_equal(getDefaultRegistry()$seed, 123L)

  batchtools$default.registry = prev
})

test_that("extra files are loaded", {
  wd = fs::file_temp()
  fs::dir_create(fs::path(wd, "subdir"))

  # define some files to source/load
  fn = list(source = fs::path(wd, "src_file.r"), load = fs::path(wd, "subdir", "load_file.RData"))
  writeLines("x_from_source = 123", con = fn$source)
  x_from_load = 321
  save(x_from_load, file = fn$load)
  rm(x_from_load)

  reg = makeTestRegistry(work.dir = wd, source = fn$source, load = fn$load)
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)

  reg = makeTestRegistry(work.dir = wd, source = fs::path_file(fn$source), load = fs::path("subdir", fs::path_file(fn$load)))
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)
  fs::dir_delete(wd)
})

test_that("loadRegistry", {
  regs = list(
    makeTestRegistry(),
    makeTestExperimentRegistry()
  )
  for (reg1 in regs) {
    fd = reg1$file.dir
    setDefaultRegistry(NULL)
    reg2 = loadRegistry(fd, make.default = FALSE, writeable = TRUE)
    checkTables(reg1)
    checkTables(reg2)
    nms = union(ls(reg1, all.names = TRUE), ls(reg2, all.names = TRUE))
    nms = chsetdiff(nms, "hash")
    for (nm in nms)
      expect_equal(reg1[[nm]], reg2[[nm]], info = nm)

    x = readRDS(fs::path(fd, "registry.rds"))
    expect_null(x$cluster.functions)
    expect_null(x$default.resources)
    expect_null(x$temp.dir)
    expect_null(x$mtime)
    expect_null(x$writeable)
  }
})

test_that("loadRegistry with missing dependencies is still usable (#122)", {
  expect_warning(reg <- makeTestRegistry(source = fs::file_temp()), "Failed to source")
  saveRegistry(reg)
  expect_warning(loadRegistry(reg$file.dir, writeable = TRUE), "Failed to source")
  batchMap(identity, 1, reg = reg)
  expect_error(testJob(1, external = FALSE, reg = reg), "Failed to source file")
})

test_that("loadRegistry after early node error still usable (#135)", {
  reg = makeTestRegistry()
  batchMap(identity, 1:2, reg = reg)
  jc = makeJobCollection(1, reg = reg)
  jc$packages = "not_existing_package"
  suppressAll(doJobCollection(jc))
  expect_character(list.files(fs::path(reg$file.dir, "updates")), len = 1L)
  expect_true(syncRegistry(reg = reg))
  expect_string(getErrorMessages(reg = reg)$message, fixed = "not_existing_package")
})

test_that("syncRegistry skips broken update files)", {
  reg = makeTestRegistry()
  p = dir(reg, "updates")
  fs::file_create(fs::path(p, "foo.rds"))
  fs::dir_ls(p)
  expect_message(syncRegistry(reg = reg), "Skipping")
})

test_that("clearRegistry", {
  reg = makeTestRegistry()
  reg$foo = TRUE
  ids = batchMap(identity, 1:3, reg = reg)
  addJobTags(1:2, "bar", reg = reg)
  ids[, chunk := chunk(job.id, n.chunks = 2)]
  submitAndWait(reg, ids)

  clearRegistry(reg)
  checkTables(reg, nrow = 0L)

  expect_identical(list.files(dir(reg, "jobs")), character(0))
  expect_identical(list.files(dir(reg, "logs")), character(0))
  expect_identical(list.files(dir(reg, "results")), character(0))
  expect_identical(list.files(dir(reg, "updates")), character(0))
  expect_false(fs::file_exists(fs::path(reg$file.dir, "user.function.rds")))

  expect_identical(batchMap(identity, 1:4, reg = reg), data.table(job.id = 1:4, key = "job.id"))
  expect_true(reg$foo)
})

test_that("read only mode", {
  f = function(x) if (x == 3) stop(3) else x
  reg = makeTestRegistry()
  batchMap(f, 1:4, reg = reg)
  submitAndWait(ids = 1:3, reg)

  # simulate that job 4 has been started but is not terminated yet
  jc = makeJobCollection(4L, reg = reg)
  suppressAll({doJobCollection(jc,  jc$log.file)})
  reg$status[job.id == 4L, job.hash := jc$job.hash]
  saveRegistry(reg = reg)

  reg = loadRegistry(reg$file.dir, writeable = FALSE)

  # query status
  expect_class(getStatus(reg = reg), "Status")
  expect_data_table(findDone(reg = reg), nrow = 3)
  expect_data_table(findErrors(reg = reg), nrow = 1)
  expect_character(fs::dir_ls(fs::path(reg$file.dir, "updates")), len = 1L)

  # load results
  expect_identical(loadResult(1L, reg = reg), 1L)
  expect_identical(reduceResultsList(reg = reg), as.list(c(1:2, 4L)))
  expect_character(fs::dir_ls(fs::path(reg$file.dir, "updates")), len = 1L)

  # inspect errors
  expect_data_table(getErrorMessages(reg = reg), nrow = 1)
  expect_character(getLog(3L, reg = reg))
  expect_character(getLog(4L, reg = reg))

  # try to write
  expect_error(sweepRegistry(reg = reg), "writeable")
  expect_error(setJobNames(ids = 1L, reg = reg), "writeable")
  expect_error(addJobTags(ids = 1L, "a", reg = reg), "writeable")
  expect_error(resetJobs(reg = reg), "writeable")
  expect_error(clearRegistry(reg = reg), "writeable")
  expect_error(removeRegistry(reg = reg), "writeable")
  expect_error(killJobs(reg = reg), "writeable")
  expect_directory_exists(reg$file.dir)
  expect_character(fs::dir_ls(fs::path(reg$file.dir, "updates")), len = 1L)

  # same stuff for ExperimentRegistry
  reg = makeTestExperimentRegistry()
  addProblem("foo", data = 1, reg = reg)
  addAlgorithm("bar", function(data, instance, ...) instance, reg = reg)
  addExperiments(reg = reg)
  reg$writeable = FALSE

  expect_data_table(summarizeExperiments(reg = reg), nrow = 1L)
  expect_data_table(findExperiments(reg = reg))

  expect_error(addProblem("foo2", iris, reg = reg), "writeable")
  expect_error(removeProblems("foo2", reg = reg), "writeable")
  expect_error(addAlgorithm("bar2", function(data, instance, ...) instance, reg = reg), "writeable")
  expect_error(removeAlgorithms("bar2", reg = reg), "writeable")
  expect_error(addExperiments(reg = reg), "writeable")
  expect_error(removeExperiments(1, reg = reg), "writeable")
})

test_that("xz compression", {
  fn = fs::file_temp("conf")
  writeLines(con = fn, "compress = \"xz\"")
  reg = makeTestRegistry(conf.file = fn)
  expect_identical(reg$compress, "xz")
  fd = file(dir(reg, "registry.rds"), "r")
  expect_identical(summary(fd)$class, "xzfile")
  close(fd)

  batchMap(identity, 1:3, reg = reg)
  submitAndWait(reg)

  fd = file(getResultFiles(reg, 1), "r")
  expect_identical(summary(fd)$class, "xzfile")
  close(fd)
})
