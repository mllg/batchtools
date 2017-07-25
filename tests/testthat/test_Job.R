context("Job")

test_that("Job", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))
  submitAndWait(reg, 1, resources = list(foo = "bar"))

  job = makeJob(reg = reg, i = 1)
  expect_is(job, "Job")
  expect_identical(job$id, 1L)
  expect_equal(job$pars, list(i = 1L, x = 1))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_equal(job$resources$foo, "bar")
  expect_function(job$fun)

  jc = makeJobCollection(reg = reg, resources = list(foo = "bar"))
  job = getJob(jc, i = 1L)
  expect_is(job, "Job")
  expect_identical(job$id, 1L)
  expect_equal(job$pars, list(i = 1L, x = 1))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_equal(job$resources$foo, "bar")
  expect_function(job$fun)
})

test_that("Experiment", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  addProblem(reg = reg, "p1", fun = function(job, data, ...) list(data = data, ...))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) length(instance))
  ids = addExperiments(list(p1 = data.table(i = 1:3)), list(a1 = data.table()), reg = reg)

  job = makeJob(1, reg = reg)
  expect_is(job, "Experiment")
  expect_identical(job$id, 1L)
  expect_equal(job$pars, list(prob.pars = list(i = 1), algo.pars = list()))
  expect_count(job$repl)
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_is(job$problem, "Problem")
  expect_is(job$algorithm, "Algorithm")
  expect_identical(job$instance, list(data = NULL, i = 1L))

  jc = makeJobCollection(reg = reg)
  job = getJob(jc, i = 1L)
  expect_is(job, "Experiment")
  expect_identical(job$id, 1L)
  expect_equal(job$pars, list(prob.pars = list(i = 1), algo.pars = list()))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_is(job$problem, "Problem")
  expect_is(job$algorithm, "Algorithm")
  expect_identical(job$instance, list(data = NULL, i = 1L))
})

test_that("External directory is created", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(..., .job) .job$external.dir
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))
  submitAndWait(reg)
  expect_directory_exists(reduceResultsDataTable(1:3, reg = reg)[[2]])

  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  addProblem(reg = reg, "p1", fun = function(job, data, ...) list(data = data, ...))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) {
    saveRDS(job$id, file = fp(job$external.dir, sprintf("%s.rds", job$id)))
    job$external.dir
  })
  ids = addExperiments(list(p1 = data.table(i = 1:3)), list(a1 = data.table()), reg = reg)
  submitAndWait(reg, c(1, 3))
  paths = reduceResultsList(1:3, missing.val = NULL, reg = reg)
  expect_directory_exists(paths[[1]])
  expect_true(file.exists(fp(reg$file.dir, "external", "1", "1.rds")))
  expect_null(paths[[2]])
  expect_false(dir.exists(fp(reg$file.dir, "external", "2")))
  expect_directory_exists(paths[[3]])
  expect_true(file.exists(fp(reg$file.dir, "external", "3", "3.rds")))
  expect_equal(reduceResultsList(1:3, fun = function(job, ...) job$external.dir, reg = reg, missing.val = NULL), paths)
  resetJobs(3, reg = reg)
  expect_false(dir.exists(fp(reg$file.dir, "external", "3")))
  expect_true(dir.exists(fp(reg$file.dir, "external", "1")))

  # directory is persistent between submits?
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) {
    list.files(job$external.dir)
  })

  submitAndWait(reg, 1)
  sweepRegistry(reg = reg)
  expect_true(file.exists(fp(reg$file.dir, "external", "1", "1.rds")))
  expect_identical(loadResult(1, reg = reg), "1.rds")
})
