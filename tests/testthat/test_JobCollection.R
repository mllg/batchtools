context("JobCollection")

test_that("makeJobCollection", {
  reg = makeTestRegistry()
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))

  jc = makeJobCollection(ids, resources = list(foo = 42), reg = reg)
  expect_environment(jc, c("file.dir", "job.hash", "jobs", "log.file", "packages", "resources", "uri", "work.dir"))

  expect_directory(jc$file.dir)
  expect_string(jc$job.hash, pattern = "^job[[:alnum:]]{32}$")
  expect_data_table(jc$jobs, key = "job.id")
  expect_string(jc$log.file)
  expect_character(jc$packages, any.missing = FALSE)
  expect_list(jc$resources, names = "unique")
  expect_string(jc$uri)
  expect_directory(jc$work.dir)
  expect_list(jc$jobs$job.pars)
  expect_string(jc$array.var, na.ok = TRUE)
  expect_flag(jc$array.jobs)

  expect_output(print(jc), "Collection")
})


test_that("makeJobCollection does not expand relative paths", {
  skip_on_os("windows")
  reg = makeTestRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, 1, reg = reg)
  reg$file.dir = npath("~/foo", must.work = FALSE)
  reg$work.dir = npath("~/bar", must.work = FALSE)
  jc = makeJobCollection(1, reg = reg)
  expect_true(stri_startswith_fixed(jc$file.dir, "~/foo"))
  expect_true(stri_startswith_fixed(jc$uri, "~/foo/jobs/"))
  expect_true(stri_startswith_fixed(jc$log.file, "~/foo/logs"))
  expect_true(stri_startswith_fixed(jc$work.dir, "~/bar"))
})

test_that("makeJobCollection.ExperimentCollection", {
  reg = makeTestExperimentRegistry()
  addProblem(reg = reg, "p1", fun = function(job, data, ...) list(data = data, ...))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) length(instance))
  ids = addExperiments(list(p1 = data.table(i = 1:3)), list(a1 = data.table()), reg = reg)

  jc = makeJobCollection(ids, resources = list(foo = 42), reg = reg)

  expect_directory(jc$file.dir)
  expect_string(jc$job.hash, pattern = "^job[[:alnum:]]{32}$")
  expect_data_table(jc$jobs, key = "job.id")
  expect_string(jc$log.file)
  expect_character(jc$packages, any.missing = FALSE)
  expect_list(jc$resources, names = "unique")
  expect_string(jc$uri)
  expect_directory(jc$work.dir)
  expect_list(jc$jobs$prob.pars)
  expect_list(jc$jobs$algo.pars)
  expect_character(jc$jobs$problem)
  expect_character(jc$jobs$algorithm)
  expect_string(jc$array.var, na.ok = TRUE)
  expect_flag(jc$array.jobs)

  expect_is(jc, "ExperimentCollection")
})
