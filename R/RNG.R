convertSeed = function(seed, method = "default") {
  if (identical(method, "lecuyer") && length(seed) == 1L) {
    # convert integer seed to state
    kind = RNGkind()
    if (!identical(kind[1L], "L'Ecuyer-CMRG")) {
      RNGkind("L'Ecuyer-CMRG")
      on.exit(RNGkind(kind = kind[1L], normal.kind = kind[2L]))
    }
    set.seed(seed)
    seed = get0(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
  }
  seed
}

#' @useDynLib batchtools next_streams
incrementSeed = function(seed, i) {
  if (length(seed) == 1L) {
    ifelse(i > .Machine$integer.max - seed, seed - .Machine$integer.max + i, seed + i)
  } else {
    assertInteger(seed, len = 7L, any.missing = FALSE)
    stopifnot(!is.unsorted(i))
    .Call(next_streams, seed, as.integer(i))
  }
}

with_temp_rng = function(type, expr) {
  saved.kind = RNGkind()
  saved.state = get0(".Random.seed", envir = .GlobalEnv, inherits = FALSE, ifnotfound = NULL)
  on.exit({
    RNGkind(saved.kind[1L], saved.kind[2L])
    if (is.null(saved.state))
      rm(".Random.seed", envir = .GlobalEnv)
    else
      assign(".Random.seed", saved.state, envir = .GlobalEnv)
  })
  RNGkind(type)
  force(expr)
}

with_seed = function(seed, expr) {
  if (length(seed) == 1L) { # Mersenne
    set.seed(seed, kind = "Mersenne-Twister")
  } else { # L'Ecuyer
    assign(".Random.seed", seed, envir = .GlobalEnv)
  }
  eval.parent(expr)
}


getSeed = function(start.seed, id) {
  if (id > .Machine$integer.max - start.seed)
    start.seed - .Machine$integer.max + id
  else
    start.seed + id
}

with_seed = function(seed, expr) {
  if (!is.null(seed)) {
    if (!exists(".Random.seed", .GlobalEnv))
      set.seed(NULL)
    state = get(".Random.seed", .GlobalEnv)
    set.seed(seed)
    on.exit(assign(".Random.seed", state, envir = .GlobalEnv))
  }
  eval.parent(expr)
}
