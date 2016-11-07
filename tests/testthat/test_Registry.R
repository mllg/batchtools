context("Registry")

test_that("makeRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  expect_is(reg, "Registry")
  expect_true(is.environment(reg))
  expect_directory(reg$file.dir, access = "rw")
  expect_directory(reg$work.dir, access = "r")
  expect_directory(file.path(reg$file.dir, c("jobs", "results", "updates", "logs")))
  expect_file(file.path(reg$file.dir, "registry.rds"))
  expect_character(reg$packages, any.missing = FALSE)
  expect_character(reg$namespaces, any.missing = FALSE)
  expect_int(reg$seed, na.ok = FALSE)
  expect_true(reg$writeable)
  expect_is(reg$cluster.functions, "ClusterFunctions")
  expect_list(reg$default.resources, names = "strict")
  checkTables(reg, any.missing = FALSE, nrows = 0L)

  reg = makeRegistry(file.dir = NA, make.default = FALSE, packages = "checkmate", seed = 123)
  expect_equal(reg$packages, "checkmate")
  expect_int(reg$seed)
  expect_identical(reg$seed, 123L)

  expect_output(print(reg), "Registry")
})

test_that("reading conf file", {
  fn = tempfile("conf")
  writeLines(con = fn, "default.resources = list(walltime = 42)")
  reg = makeRegistry(file.dir = NA, make.default = FALSE, conf.file = fn)
  expect_identical(reg$default.resources, list(walltime = 42))
})

test_that("make.default does work", {
  if (!interactive()) {
    setDefaultRegistry(NULL)
    expect_error(getDefaultRegistry(), "No default")
    reg = makeRegistry(file.dir = NA, make.default = TRUE, seed = 123)
    expect_equal(reg$seed, 123L)
    reg = makeRegistry(file.dir = NA, make.default = FALSE, seed = 124)
    expect_equal(reg$seed, 124L)
    expect_equal(getDefaultRegistry()$seed, 123L)

    expect_null(setDefaultRegistry(NULL))
    expect_error(getDefaultRegistry(), "No default")
    setDefaultRegistry(reg)
    expect_class(getDefaultRegistry(), "Registry")
  }
})

test_that("extra files are loaded", {
  wd = tempfile()
  dir.create(wd, recursive = TRUE)
  dir.create(file.path(wd, "subdir"), recursive = TRUE)

  # define some files to source/load
  fn = list(source = file.path(wd, "src_file.r"), load = file.path(wd, "subdir", "load_file.RData"))
  writeLines("x_from_source = 123", con = fn$source)
  x_from_load = 321
  save(x_from_load, file = fn$load)
  rm(x_from_load)

  reg = makeRegistry(file.dir = NA, make.default = FALSE, work.dir = wd, source = fn$source, load = fn$load)
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)

  reg = makeRegistry(file.dir = NA, make.default = FALSE, work.dir = wd, source = basename(fn$source), load = file.path("subdir", basename(fn$load)))
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)
})

test_that("loadRegistry", {
  reg1 = makeRegistry(file.dir = NA, make.default = FALSE)
  fd = reg1$file.dir
  setDefaultRegistry(NULL)
  reg2 = loadRegistry(fd, make.default = FALSE)
  checkTables(reg2)
  expect_equal(reg1, reg2)

  x = readRDS(file.path(fd, "registry.rds"))
  expect_null(x$cluster.functions)
})

test_that("sweepRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, 1, reg = reg)

  submitAndWait(reg, 1, resources = list(foo = 1))
  submitAndWait(reg, 1, resources = list(foo = 2))
  writeRDS(makeJobCollection(1, reg = reg), file.path(reg$file.dir, "jobs", "test.rds"))

  expect_data_table(reg$resources, nrow = 2)
  expect_character(list.files(file.path(reg$file.dir, "logs")), len = 2L)
  if (reg$cluster.functions$store.job)
    expect_character(list.files(file.path(reg$file.dir, "jobs")), len = 1L)

  expect_true(sweepRegistry(reg), "Registry")

  expect_data_table(reg$resources, nrow = 1)
  expect_character(list.files(file.path(reg$file.dir, "logs")), len = 1L)
  if (reg$cluster.functions$store.job)
    expect_character(list.files(file.path(reg$file.dir, "jobs")), len = 0L)
  checkTables(reg)


  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  addExperiments(prob.designs = list(p1 = data.table(i = 1:10)), reg = reg)
  addJobTags(6:10, "foo", reg = reg)
  expect_data_table(reg$tags, nrow = 5, any.missing = FALSE)
  removeExperiments(ids = 6:10, reg = reg)
  expect_data_table(reg$tags, nrow = 0)
})

test_that("clearRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$foo = TRUE
  batchMap(identity, 1:3, reg = reg)
  addJobTags(1:2, "bar", reg = reg)
  submitAndWait(reg, chunkIds(reg = reg, n.chunks = 2))

  clearRegistry(reg)
  checkTables(reg, nrow = 0L)

  expect_identical(list.files(file.path(reg$file.dir, "jobs")), character(0))
  expect_identical(list.files(file.path(reg$file.dir, "logs")), character(0))
  expect_identical(list.files(file.path(reg$file.dir, "results")), character(0))
  expect_identical(list.files(file.path(reg$file.dir, "updates")), character(0))
  expect_false(file.exists(file.path(reg$file.dir, "user.function.rds")))

  expect_identical(batchMap(identity, 1:4, reg = reg), data.table(job.id = 1:4, key = "job.id"))
  expect_true(reg$foo)
})
