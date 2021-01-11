test_that("addProblem / removeProblem", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  expect_is(prob, "Problem")
  expect_equal(prob$data, iris)
  expect_equal(prob$name, "p1")
  expect_function(prob$fun)
  expect_null(prob$seed)
  expect_file_exists(getProblemURI(reg, prob$name))
  expect_false(prob$cache)
  expect_false(fs::dir_exists(getProblemCacheDir(reg, "p1")))

  prob = addProblem(reg = reg, "p2", fun = function(...) NULL, seed = 42, cache = TRUE)
  expect_is(prob, "Problem")
  expect_null(prob$data, NULL)
  expect_equal(prob$name, "p2")
  expect_function(prob$fun)
  expect_identical(prob$seed, 42L)
  expect_file_exists(getProblemURI(reg, prob$name))
  expect_true(prob$cache)
  expect_directory_exists(getProblemCacheDir(reg, "p2"))

  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  prob.designs = list(p1 = data.table(), p2 = data.table())
  algo.designs = list(a1 = data.table())
  ids = addExperiments(prob.designs, algo.designs, repls = 2, reg = reg)
  expect_integer(ids$job.id, len = 4L)

  removeProblems(reg = reg, "p1")
  expect_directory_exists(getProblemCacheDir(reg, "p2"))
  expect_integer(reg$status$job.id, len = 2L)
  expect_set_equal("p2", reg$problems)
  expect_false(fs::file_exists(getProblemURI(reg, "p1")))
  expect_true(fs::file_exists(getProblemURI(reg, "p2")))
  expect_set_equal(getJobPars(reg = reg)$problem, "p2")
  checkTables(reg)

  removeProblems(reg = reg, "p2")
  expect_false(fs::dir_exists(getProblemCacheDir(reg, "p2")))
})


test_that("instance caching", {
  reg = makeTestExperimentRegistry()
  addProblem(reg = reg, "p1", data = iris, fun = function(job, data, param) param * 10 + runif(1), seed = 1, cache = TRUE)
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) list(result = instance))
  addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, ...) list(result = instance))
  ids = addExperiments(prob.designs = list(p1 = data.table(param = 1:2)), repls = 2, reg = reg)

  j = makeJob(1, reg = reg)
  foo = j$instance
  expect_file_exists(getProblemCacheURI(j))

  submitAndWait(reg = reg)
  tab = unwrap(ljoin(getJobTable(reg = reg)[, c("job.id", "repl", "problem", "prob.pars", "algorithm")], reduceResultsDataTable(reg = reg)))
  expect_equal(tab[, list(v = var(result)), by = c("param", "problem", "repl")]$v, rep(0, 4))
})
