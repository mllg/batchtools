context("Seeds")

test_that("with_seed", {
  set.seed(1)
  x.1 = runif(5)
  set.seed(42)
  x.42 = runif(5)
  x.next = runif(5)

  set.seed(42)
  y.1 = with_seed(1, runif(5))
  y.42 = runif(5)
  y.next = runif(5)

  expect_identical(x.1, y.1)
  expect_identical(x.42, y.42)
  expect_identical(x.next, y.next)
  expect_error(with_seed(1, print(state)))
})

test_that("Problem and Algorithm seed", {
  reg = makeTestExperimentRegistry(seed = 42)
  addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) runif(1), seed = 1L)
  addProblem(reg = reg, "p2", data = iris, fun = function(job, data, ...) runif(1))
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) list(instance = instance, res = runif(1)))
  addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, ...) list(instance = instance, res = runif(1)))
  prob.designs = list(p1 = data.table(), p2 = data.table())
  algo.designs = list(a1 = data.table(), a2 = data.table())
  repls = 3
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)

  submitAndWait(reg, ids)

  set.seed(1); p1 = runif(1)
  set.seed(2); p2 = runif(1)
  set.seed(3); p3 = runif(1)
  set.seed(43); a1 = runif(1)
  set.seed(44); a2 = runif(1)
  set.seed(45); a3 = runif(1)
  silent({
    ids = findExperiments(algo.name = "a1", prob.name = "p1", reg = reg)
    results = rbindlist(reduceResultsList(ids, reg = reg))
  })
  expect_true(all(results$instance == c(p1, p2, p3)))
  expect_true(all(results$res == c(a1, a2, a3)))

  silent({
    ids = findExperiments(prob.name = "p1", repl = 2, reg = reg)
    results = rbindlist(reduceResultsList(ids, reg = reg))
  })
  expect_true(all(results$instance == p2))

  silent({
    ids = findExperiments(prob.name = "p2", reg = reg)
    results = rbindlist(reduceResultsList(ids, reg = reg))
  })
  expect_numeric(results$instance, unique = TRUE)
  expect_numeric(results$res, unique = TRUE)
})
