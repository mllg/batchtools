context("testJob")

test_that("testJob", {
  reg = makeTestRegistry()
  f = function(x) if (x %% 2 == 0) stop("foo") else x^2
  batchMap(reg = reg, f, 1:3)
  expect_equal(testJob(reg = reg, id = 1), 1)
  expect_equal(testJob(reg = reg, id = 3), 9)
  expect_error(testJob(reg = reg, id = 2), "foo")

  expect_equal(suppressAll(testJob(reg = reg, id = 1, external = TRUE)), 1)
  expect_error(suppressAll(testJob(reg = reg, id = 2, external = TRUE)), "re-run")

  expect_equal(findSubmitted(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
  expect_equal(findDone(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
  expect_equal(findErrors(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
})

test_that("testJob.ExperimentRegistry", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq, ...) instance^sq)
  ids = addExperiments(prob.designs = list(p1 = data.table()), algo.designs = list(a1 = data.table(sq = 1:3)), reg = reg)

  suppressAll(x <- testJob(id = 1, reg = reg))
  expect_equal(x, 150)
  suppressAll(x <- testJob(id = 2, reg = reg, external = TRUE))
  expect_equal(x, 150^2)
})

test_that("traceback works in external session", {
  reg = makeTestRegistry()
  f = function(x) {
    g = function(x) findme(x)
    findme = function(x) h(x)
    h = function(x) stop("Error in h")
    g(x)
  }
  batchMap(f, 1, reg = reg)
  expect_output(expect_error(testJob(1, external = TRUE, reg = reg), "external=FALSE"), "findme")
})
