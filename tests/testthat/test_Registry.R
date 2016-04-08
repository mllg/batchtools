context("Registry")

test_that("makeRegistry", {
  reg = makeTempRegistry(FALSE)
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
  expect_false(reg$debug)
  expect_is(reg$cluster.functions, "ClusterFunctions")
  expect_list(reg$default.resources, names = "strict")
  checkTables(reg, any.missing = FALSE, nrows = 0L)

  reg = makeTempRegistry(FALSE, packages = "checkmate", seed = 123)
  expect_equal(reg$packages, "checkmate")
  expect_int(reg$seed)
  expect_identical(reg$seed, 123L)

  expect_output(print(reg), "Registry")
})

test_that("make.default does work", {
  if (!interactive()) {
    expect_error(getDefaultRegistry(), "No default")
    reg = makeTempRegistry(TRUE, packages = "checkmate", seed = 123)
    expect_equal(reg$seed, 123L)
    reg = makeTempRegistry(FALSE, seed = 124L)
    expect_equal(reg$seed, 124L)
    expect_equal(getDefaultRegistry()$seed, 123L)

    expect_true(clearDefaultRegistry())
    expect_error(getDefaultRegistry(), "No default")
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

  reg = makeTempRegistry(FALSE, work.dir = wd, source = fn$source, load = fn$load)
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)

  reg = makeTempRegistry(FALSE, work.dir = wd, source = basename(fn$source), load = file.path("subdir", basename(fn$load)))
  expect_identical(get("x_from_source", .GlobalEnv), 123)
  expect_identical(get("x_from_load", .GlobalEnv), 321)
  rm("x_from_source", envir = .GlobalEnv)
  rm("x_from_load", envir = .GlobalEnv)
})

test_that("loadRegistry", {
  reg1 = makeTempRegistry(FALSE)
  fd = reg1$file.dir
  clearDefaultRegistry()
  reg2 = loadRegistry(fd, make.default = FALSE)
  checkTables(reg2)
  expect_equal(reg1, reg2)
})
