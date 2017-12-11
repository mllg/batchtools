context("Registry")

test_that("makeRegistry", {
  reg = makeTestRegistry()
  expect_is(reg, "Registry")
  expect_true(is.environment(reg))
  expect_directory(reg$file.dir, access = "rw")
  expect_directory(reg$work.dir, access = "r")
  expect_directory(fp(reg$file.dir, c("jobs", "results", "updates", "logs")))
  expect_file(fp(reg$file.dir, "registry.rds"))
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
  fn = tempfile("conf")
  writeLines(con = fn, "default.resources = list(walltime = 42)")
  reg = makeTestRegistry(conf.file = fn)
  expect_identical(reg$default.resources, list(walltime = 42))
  file.remove(fn)
})

test_that("make.default does work", {
  if (!interactive()) {
    setDefaultRegistry(NULL)
    expect_error(getDefaultRegistry(), "No default")
    reg = makeTestRegistry(make.default = TRUE, seed = 123)
    expect_equal(reg$seed, 123L)
    reg = makeTestRegistry(seed = 124)
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
  dir.create(fp(wd, "subdir"), recursive = TRUE)

  # define some files to source/load
  fn = list(source = fp(wd, "src_file.r"), load = fp(wd, "subdir", "load_file.RData"))
  writeLines("x_from_source = 123", con = fn$source)
  x_from_load = 321
  save(x_from_load, file = fn$load)
  rm(x_from_load)

  reg = makeTestRegistry(work.dir = wd, source = fn$source, load = fn$load)
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)

  reg = makeTestRegistry(work.dir = wd, source = basename(fn$source), load = fp("subdir", basename(fn$load)))
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)
  unlink(wd, recursive = TRUE)
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

    x = readRDS(fp(fd, "registry.rds"))
    expect_null(x$cluster.functions)
    expect_null(x$default.resources)
    expect_null(x$temp.dir)
    expect_null(x$mtime)
    expect_null(x$writeable)
  }
})

test_that("loadRegistry with missing dependencies is still usable (#122)", {
  expect_warning(reg <- makeTestRegistry(source = tempfile()), "Failed to source")
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
  expect_character(list.files(fp(reg$file.dir, "updates")), len = 1L)
  expect_true(syncRegistry(reg = reg))
  expect_string(getErrorMessages(reg = reg)$message, fixed = "not_existing_package")
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
  expect_false(file.exists(fp(reg$file.dir, "user.function.rds")))

  expect_identical(batchMap(identity, 1:4, reg = reg), data.table(job.id = 1:4, key = "job.id"))
  expect_true(reg$foo)
})
