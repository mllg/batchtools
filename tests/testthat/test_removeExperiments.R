context("removeExperiments")

test_that("removeExperiments", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) nrow(data), seed = 42)
  prob = addProblem(reg = reg, "p2", data = iris, fun = function(job, data) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  algo = addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, sq) instance^sq)
  ids = addExperiments(list(p1 = data.table(), p2 = data.table(x = 1:2)), list(a1 = data.table(sq = 1:3), a2 = data.table(sq = 1:2)), reg = reg)
  N = nrow(findExperiments(reg = reg))

  expect_data_table(removeExperiments(1, reg = reg), nrow = 1, key = "job.id")
  expect_equal(findExperiments(reg = reg)$job.id, 2:N)
  expect_data_table(removeExperiments(1, reg = reg), nrow = 0, key = "job.id")
  expect_equal(findExperiments(reg = reg)$job.id, 2:N)

  ids = findExperiments(prob.name = "p1", reg = reg)
  expect_data_table(removeExperiments(ids, reg = reg), nrow = 4, key = "job.id")
  expect_equal(findExperiments(reg = reg)$job.id, 6:N)
  expect_true(file.exists(getProblemURI(reg, "p1")))
  expect_set_equal(c("p1", "p2"), reg$problems)

  ids = findExperiments(algo.name = "a2", reg = reg)
  expect_data_table(removeExperiments(ids, reg = reg), nrow = 4, key = "job.id")
  expect_equal(findExperiments(reg = reg)$job.id, 6:(N-nrow(ids)))
  expect_true(file.exists(getAlgorithmURI(reg, "a2")))
  expect_set_equal(c("a1", "a2"), reg$algorithms)

  checkTables(reg)
})
