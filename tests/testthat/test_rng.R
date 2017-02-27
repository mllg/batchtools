context("RNG")

with_rng = function(kind, expr) {
  prev = RNGkind()[1L]
  on.exit(RNGkind(prev))
  RNGkind(kind)
  force(expr)
}

getState = function() {
  get0(".Random.seed", envir = .GlobalEnv, ifnotfound = NULL)
}

setState = function(state) {
  assign(".Random.seed", state, envir = .GlobalEnv)
}

getStream = function(start, i) {
  with_rng("L'Ecuyer-CMRG", {
    set.seed(start)
    state = getState()
    expected = matrix(NA, nrow = 7, ncol = length(i))
    for (ii in order(i)) {
      state = parallel::nextRNGStream(state)
      expected[, ii] = state
    }
    expected
  })
}

test_that("next_streams is identical to parallel's stream (precomputed)", {
  expected = c(407L, 1801422725L, -2057975723L, 1156894209L, 1595475487L, 210384600L, -1655729657L)
  prev.kind = RNGkind()

  with_rng("L'Ecuyer-CMRG", {
    set.seed(123)
    seed = getState()
    expect_equal(parallel::nextRNGStream(seed), expected)
  })
  expect_equal(RNGkind(), prev.kind)
})

test_that("same streams for multiple i", {
  i = sample(1:50)
  expected = getStream(123, i)

  rng = getRNG("lecuyer", 123, i)
  expect_equal(expected, rng$states)
  expect_equal(RNGkind(), c("L'Ecuyer-CMRG", "Inversion"))
  rng$restore()
})


test_that("Mersenne", {
  start = sample(10000L, 1L)
  prev.kind = RNGkind()
  prev.state = getState()

  i = c(3, 1, 2, 4)
  rng = getRNG("mersenne", start, i)
  expect_equal(rng$states, start + i)

  state1 = getState()
  setState(state1)
  x1 = runif(1)
  rng$nextStream()
  state2 = getState()
  setState(state2)
  x2 = runif(1)

  rng$restore()
  expect_equal(RNGkind(), prev.kind)
  expect_equal(prev.state, getState())

  set.seed(start + i[1], kind = rng$kind)
  expect_equal(getState(), state1)
  expect_equal(x1, runif(1))
  set.seed(start + i[2], kind = rng$kind)
  expect_equal(getState(), state2)
  expect_equal(x2, runif(1))

  rng$restore()
})

test_that("L'Ecuyer", {
  start = sample(10000L, 1L)
  prev.kind = RNGkind()
  prev.state = getState()

  i = c(3, 1, 2, 4)
  rng = getRNG("lecuyer", start, i)
  expect_matrix(rng$states, nrow = 7, ncol = 4, any.missing = FALSE)
  expect_true(all(rng$states[1, ] %% 100 == 7))

  state1 = getState()
  setState(state1)
  x1 = runif(1)
  rng$nextStream()
  state2 = getState()
  setState(state2)
  x2 = runif(1)

  rng$restore()
  expect_equal(RNGkind(), prev.kind)
  expect_equal(prev.state, getState())

  set.seed(start, kind = rng$kind)
  state = getState()
  for (j in seq_len(i[1])) state = parallel::nextRNGStream(state)

  expect_equal(state, rng$states[, 1])
  expect_equal(state, state1)
  setState(state1)
  expect_equal(x1, runif(1))

  set.seed(start, kind = rng$kind)
  state = getState()
  for (j in seq_len(i[2])) state = parallel::nextRNGStream(state)
  expect_equal(state, rng$states[, 2])
  expect_equal(state, state2)
  setState(state2)
  expect_equal(x2, runif(1))

  rng$restore()
})

test_that("Problem and Algorithm seed", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE, seed = 42)
  addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) runif(1), seed = 1L)
  addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) list(instance = instance, res = runif(1)))
  addAlgorithm(reg = reg, "a2", fun = function(job, data, instance, ...) list(instance = instance, res = runif(1)))
  prob.designs = list(p1 = data.table())
  algo.designs = list(a1 = data.table(), a2 = data.table())
  repls = 3
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)

  submitAndWait(reg, ids)

  set.seed(2); p1 = runif(1)
  set.seed(3); p2 = runif(1)
  set.seed(4); p3 = runif(1)
  set.seed(43); a1 = runif(1)
  set.seed(44); a2 = runif(1)
  set.seed(45); a3 = runif(1)
  silent({
    ids = findExperiments(algo.name = "a1", reg = reg)
    results = rbindlist(reduceResultsList(ids, reg = reg))
  })
  expect_true(all(results$instance == c(p1, p2, p3)))
  expect_true(all(results$res == c(a1, a2, a3)))

  silent({
    ids = findExperiments(repl = 2, reg = reg)
    results = rbindlist(reduceResultsList(ids, reg = reg))
  })
  expect_true(all(results$instance == p2))
})
