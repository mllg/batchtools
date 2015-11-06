context("testJob")

test_that("testJob", {
  reg = makeTempRegistry(FALSE)
  f = function(x) if (x %% 2 == 0) stop("foo") else x^2
  batchMap(reg = reg, f, 1:3)
  expect_equal(testJob(reg = reg, id = 1), 1)
  expect_equal(testJob(reg = reg, id = 3), 9)
  expect_error(testJob(reg = reg, id = 2), "foo")

  expect_equal(findSubmitted(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
  expect_equal(findDone(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
  expect_equal(findError(reg = reg), data.table(job.id = integer(0L), key = "job.id"))
})

test_that("testJob.ExperimentRegistry", {
  reg = makeTempExperimentRegistry(FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, problem, sq, ...) problem^sq)
  ids = addExperiments(prob.designs = list(p1 = data.table()), algo.designs = list(a1 = data.table(sq = 1:3)), reg = reg)

  suppressAll(x <- testJob(id = 1, reg = reg))
  expect_equal(x, 150)
  suppressAll(x <- testJob(id = 2, reg = reg))
  expect_equal(x, 150^2)
})
