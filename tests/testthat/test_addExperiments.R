context("addExperiments")

test_that("addExperiments handles parameters correctly", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, x, y, ...) stopifnot(is.numeric(x) && is.character(y)), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, a, b, ...) { print(str(a)); assertList(a, len = 1, names = "named"); assertDataFrame(b); } )
  prob.designs = list(p1 = data.table(x = 1:2, y = letters[1:2]))
  algo.designs = list(a1 = data.table(a = list(list(x = 1)), b = list(iris)))
  repls = 2
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)
  expect_data_table(ids, nrow = 4, key = "job.id")
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)
  expect_data_table(ids, nrow = 0, key = "job.id")
  ids = addExperiments(prob.designs, algo.designs, repls = repls + 1L, reg = reg)
  expect_data_table(ids, nrow = 2, key = "job.id")

  submitAndWait(reg, ids)
  expect_true(nrow(findErrors(reg = reg)) == 0)
})

test_that("addExperiments creates default designs", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris)
  prob = addProblem(reg = reg, "p2", data = cars)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance) nrow(data))
  algo = addAlgorithm(reg = reg, "a2", fun = function(job, data, instance) ncol(data))
  ids = addExperiments(reg = reg)
  expect_equal(findExperiments(reg = reg)$job.id, 1:4)
  expect_equal(as.character(reg$defs$problem), rep(c("p1", "p2"), each = 2))
  expect_equal(as.character(reg$defs$algorithm), rep(c("a1", "a2"), times = 2))
})

test_that("addExperiments / user provided designs", {
  reg = makeTestExperimentRegistry()
  addProblem(reg = reg, "p1", data = iris, fun = function(...) list(...))
  addProblem(reg = reg, "p2", data = cars, fun = function(...) list(...))
  addAlgorithm(reg = reg, "a1", fun = function(...) list(...))
  addAlgorithm(reg = reg, "a2", fun = function(...) ncol(data))
  prob.designs = list(p1 = data.table(a = 1, b = 2:4))
  algo.designs = list(a1 = data.table(c = 3:8), a2 = data.table())
  repls = 1
  ids = addExperiments(reg = reg, prob.designs = prob.designs, algo.designs = algo.designs, combine = "bind")
  expect_data_table(ids, nrow = 9, key = "job.id")
  pars = unwrap(getJobPars(reg = reg))
  expect_set_equal(pars$problem, "p1")
  expect_set_equal(pars$algorithm, c("a1", "a2"))
  expect_equal(pars$a, rep(1L, 9))
  expect_equal(pars$b, rep(2:4, 3))
  expect_equal(pars$c, c(3:8, rep(NA, 3)))
  expect_error(addExperiments(reg = reg, prob.designs = list(p1 = data.table(job = 2))), "reserved keyword 'job'")
  expect_error(addExperiments(reg = reg, algo.designs = list(a2 = data.table(instance = "foo"))), "reserved keyword 'instance'")

  prob.designs = c(prob.designs, list(p2 = data.table()))
  ids = addExperiments(reg = reg, prob.designs = prob.designs, algo.designs = algo.designs, combine = "bind")
  expect_data_table(ids, nrow = 7, key = "job.id")
  expect_data_table(unwrap(getJobPars(reg = reg)), nrow = 16)

  ids = addExperiments(reg = reg, prob.designs = prob.designs, algo.designs = algo.designs, combine = "crossprod")
  expect_data_table(ids, nrow = 12, key = "job.id")
  expect_data_table(unwrap(getJobPars(reg = reg)), nrow = 28)

  pd = list(p1 = data.frame(foo = letters[1:2]))
  withr::with_options(list(stringsAsFactors = NULL), {
    expect_warning(addExperiments(reg = reg, prob.designs = pd), "stringsAsFactors")
  })
  withr::with_options(list(stringsAsFactors = TRUE), {
    expect_warning(addExperiments(reg = reg, prob.designs = pd), "stringsAsFactors")
  })
  withr::with_options(list(stringsAsFactors = FALSE), {
    addExperiments(reg = reg, prob.designs = pd)
  })
})

if (FALSE) {
  reg = makeTestExperimentRegistry()
  addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, ...) NULL)
  prob.designs = list(p1 = data.table(x = 1:500))
  algo.designs = list(a1 = data.table(y = 1:50), a2 = data.table(y = 1:20))
  repls = 2
  profvis::profvis(addExperiments(prob.designs, algo.designs = algo.designs, repls = repls, reg = reg))
  ids = findExperiments(reg = reg)
  profvis::profvis(submitJobs(ids = s.chunk(ids), reg = reg))

  profvis::profvis(unwrap(getJobPars(reg = reg)))
}
