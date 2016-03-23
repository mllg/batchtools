context("JobCollection")

test_that("makeJobCollection", {
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))

  jc = makeJobCollection(ids, resources = list(foo = 42), reg = reg)
  expect_environment(jc, c("file.dir", "job.hash", "defs", "log.file", "packages", "resources", "uri", "work.dir"))

  expect_directory(jc$file.dir)
  expect_string(jc$job.hash)
  expect_true(is.data.table(jc$defs))
  expect_string(jc$log.file)
  expect_character(jc$packages, any.missing = FALSE)
  expect_list(jc$resources, names = "unique")
  expect_string(jc$uri)
  expect_directory(jc$work.dir)
  expect_list(jc$defs$pars)

  expect_output(jc, "Collection")
})

test_that("makeJobCollection.ExperimentCollection", {
  reg = makeTempExperimentRegistry(FALSE)
  addProblem(reg = reg, "p1", fun = function(job, data, ...) list(data = data, ...))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) length(instance))
  ids = addExperiments(list(p1 = data.table(i = 1:3)), list(a1 = data.table()), reg = reg)

  j = makeJobCollection(ids, resources = list(foo = 42), reg = reg)

  expect_directory(j$file.dir)
  expect_string(j$job.hash)
  expect_true(is.data.table(j$defs))
  expect_string(j$log.file)
  expect_character(j$packages, any.missing = FALSE)
  expect_list(j$resources, names = "unique")
  expect_string(j$uri)
  expect_directory(j$work.dir)
  expect_list(j$defs$pars)
  expect_factor(j$defs$problem)
  expect_factor(j$defs$algorithm)

  expect_is(j, "ExperimentCollection")
})
