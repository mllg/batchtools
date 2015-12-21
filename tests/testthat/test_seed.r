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
  reg = makeTempExperimentRegistry(FALSE, seed = 42L)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) runif(1), seed = 1L)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) list(instance = instance, res = runif(1)))
  prob.designs = list(p1 = data.table())
  algo.designs = list(a1 = data.table())
  repls = 3
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)

  silent({
    submitJobs(chunkIds(ids, reg = reg), reg = reg)
    waitForJobs(ids, reg = reg)
  })

  set.seed(1); p = runif(1)
  set.seed(43); a1 = runif(1)
  set.seed(44); a2 = runif(1)
  set.seed(45); a3 = runif(1)
  silent({
    results = rbindlist(reduceResultsList(reg = reg))
  })
  expect_true(all(results$instance == p))
  expect_true(all(results$res == c(a1, a2, a3)))
})
