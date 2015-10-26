getSeed = function(start.seed, id) {
  if (id > .Machine$integer.max - start.seed)
    start.seed - .Machine$integer.max + id
  else
    start.seed + id
}

withSeed = function(seed, expr) {
  if (!is.null(seed)) {
    if (!exists(".Random.seed", .GlobalEnv))
      set.seed(NULL)
    state = get(".Random.seed", .GlobalEnv)
    set.seed(seed)
    on.exit(assign(".Random.seed", state, envir = .GlobalEnv))
  }
  eval.parent(expr)
}
