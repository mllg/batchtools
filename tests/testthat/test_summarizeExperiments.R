context("summarizeExperiments")

test_that("summarizeExperiments", {
  reg = makeTempExperimentRegistry(FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) nrow(data), seed = 42)
  prob = addProblem(reg = reg, "p2", data = iris, fun = function(job, data) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  ids = addExperiments(list(p1 = data.table(), p2 = data.table(x = 1:2)), list(a1 = data.table(sq = 1:3)), reg = reg)
  s = summarizeExperiments(reg = reg)
  expect_is(s, "ExperimentSummary")
  expect_true(is.data.table(s$table))
  expect_data_table(s$table, nrows = 2, ncols = 3)
  expect_equal(s$table$.count, c(3, 6))
  expect_equal(s$problems, c("p1", "p2"))
  expect_equal(s$algorithms, "a1")
  expect_equal(s$table$problem, factor(c("p1", "p2")))
  expect_equal(s$table$algorithm, factor(c("a1", "a1")))


  s = summarizeExperiments(reg = reg, by = c("problem", "algorithm", "x"))
  expect_data_table(s$table, nrows = 3, ncols = 4)
  expect_equal(s$table$.count, c(3, 3, 3))
  expect_equal(s$table$x, c(NA, 1, 2))
})
