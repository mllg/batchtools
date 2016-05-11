context("addExperiments")

test_that("addExperiments handles parameters correctly", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, x, y, ...) stopifnot(is.numeric(x) && is.character(y)), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, a, b, ...) { print(str(a)); assertList(a, len = 1, names = "named"); assertDataFrame(b); } )
  prob.designs = list(p1 = data.table(x = 1:2, y = letters[1:2]))
  algo.designs = list(a1 = data.table(a = list(list(x = 1)), b = list(iris)))
  repls = 1
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)
  silent({
    submitJobs(reg = reg, ids = chunkIds(reg = reg))
    waitForJobs(reg = reg)
  })
  expect_true(nrow(findErrors(reg = reg)) == 0)
})

test_that("addExperiments creates default designs", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance) nrow(data))
  algo = addAlgorithm(reg = reg, "a2", fun = function(job, data, instance) ncol(data))
  ids = addExperiments(reg = reg)
  expect_equal(findExperiments(reg = reg)$job.id, 1:2)
  expect_equal(as.character(reg$defs$problem), c("p1", "p1"))
  expect_equal(as.character(reg$defs$algorithm), c("a1", "a2"))
})

if (FALSE) {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, ...) NULL)
  prob.designs = list(p1 = data.table(x = 1:500))
  algo.designs = list(a1 = data.table(y = 1:20), a2 = data.table(y = 1:20))
  repls = 2
  st = system.time({addExperiments(prob.designs, algo.designs = algo.designs, repls = repls, reg = reg)})
  print(st)
}
