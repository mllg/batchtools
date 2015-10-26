context("addExperiments")

test_that("addProblem", {
  reg = makeTempExperimentRegistry(FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(...) nrow(data))
  expect_is(prob, "Problem")
  expect_equal(prob$data, iris)
  expect_equal(prob$name, "p1")
  expect_function(prob$fun)
  expect_null(prob$seed)
  expect_file(file.path(reg$file.dir, "problems", "p1.rds"))

  prob = addProblem(reg = reg, "p2", fun = function(...) NULL, seed = 42)
  expect_is(prob, "Problem")
  expect_null(prob$data, NULL)
  expect_equal(prob$name, "p2")
  expect_function(prob$fun)
  expect_identical(prob$seed, 42L)
  expect_file(file.path(reg$file.dir, "problems", "p1.rds"))

  algo = addAlgorithm(reg = reg, "a1", fun = function(...) NULL)
  ids = addExperiments(list(p1 = data.table(), p2 = data.table()), algo.designs = list(a1 = data.table()), repls = 2, reg = reg)

  removeProblem(reg = reg, "p1")
  expect_set_equal(reg$problems, "p2")
  expect_false(file.exists(file.path(reg$file.dir, "problems", "p1.rds")))
  expect_set_equal(getJobDefs(reg = reg)$problem, "p2")
  checkTables(reg)
})

test_that("addAlgorithm", {
  reg = makeTempExperimentRegistry(FALSE)
  algo = addAlgorithm(reg = reg, "a1", fun = function(...) NULL)
  expect_is(algo, "Algorithm")
  expect_equal(algo$name, "a1")
  expect_function(algo$fun)
  expect_file(file.path(reg$file.dir, "algorithms", "a1.rds"))

  prob = addProblem(reg = reg, "p1", data = iris, fun = function(...) nrow(data))
  algo = addAlgorithm(reg = reg, "a2", fun = function(...) NULL)
  ids = addExperiments(list(p1 = data.table()), algo.designs = list(a1 = data.table(), a2 = data.table()), repls = 2, reg = reg)

  removeAlgorithm(reg = reg, "a1")
  expect_set_equal(reg$algorithms, "a2")
  expect_false(file.exists(file.path(reg$file.dir, "algorithms", "a1.rds")))
  expect_set_equal(getJobDefs(reg = reg)$algorithm, "a2")
  checkTables(reg)
})

test_that("addExperiments", {
  reg = makeTempExperimentRegistry(FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(data) nrow(data), seed = 42)
  prob = addProblem(reg = reg, "p2", data = iris, fun = function(data) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(data, problem, sq) problem^sq)
  prob.designs = list(p1 = data.table(), p2 = data.table(x = 1:2))
  algo.designs = list(a1 = data.table(sq = 1:3))
  repls = 10
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)

  removeProblem("p1", reg = reg)

  getJobInfo(reg = reg)

  expect_equal(ids, data.table(job.id = 1:90, key = "job.id"))
  checkTables(reg = reg)
})
