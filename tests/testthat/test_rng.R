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
