context("Job")

test_that("Job", {
  reg = makeTempRegistry(FALSE)
  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg, more.args = list(x = 1))

  job = makeJob(reg = reg, i = 1)
  expect_is(job, "Job")
  expect_output(job, "Job with id")
  expect_identical(job$job.id, 1L)
  expect_equal(job$pars, list(i = 1L, x = 1))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_function(job$fun)

  jc = makeJobCollection(reg = reg)
  job = getJob(jc, 1)
  expect_is(job, "Job")
  expect_output(job, "Job with id")
  expect_identical(job$job.id, 1L)
  expect_equal(job$pars, list(i = 1L, x = 1))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_function(job$fun)
})

test_that("Experiment", {
  reg = makeTempExperimentRegistry(FALSE)
  addProblem(reg = reg, "p1", fun = function(job, data, ...) list(data = data, ...))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) length(instance))
  ids = addExperiments(list(p1 = data.table(i = 1:3)), list(a1 = data.table()), reg = reg)

  job = makeJob(1, reg = reg)
  expect_is(job, "Experiment")
  expect_output(job, "Experiment with id")
  expect_identical(job$job.id, 1L)
  expect_equal(job$pars, list(prob.pars = list(i = 1), algo.pars = list()))
  expect_count(job$repl)
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_is(job$problem, "Problem")
  expect_is(job$algorithm, "Algorithm")

  jc = makeJobCollection(reg = reg)
  job = getJob(jc, 1)
  expect_is(job, "Experiment")
  expect_output(job, "Experiment with id")
  expect_identical(job$job.id, 1L)
  expect_equal(job$pars, list(prob.pars = list(i = 1), algo.pars = list()))
  expect_count(job$seed)
  expect_list(job$resources, names = "named")
  expect_is(job$problem, "Problem")
  expect_is(job$algorithm, "Algorithm")
})
