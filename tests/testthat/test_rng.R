context("RNG")

test_that("c: next_streams", {
  expected = c(407L, 1801422725L, -2057975723L, 1156894209L, 1595475487L, 210384600L, -1655729657L)

  with_rng("L'Ecuyer-CMRG", {
    set.seed(123)
    seed = .GlobalEnv$.Random.seed
    expect_equal(parallel::nextRNGStream(seed), expected)
  })

  RNGkind()
})
