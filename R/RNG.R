#' @useDynLib batchtools next_streams
RNGStream = R6Class("RNGStream",
  public = list(
    start.seed = NA_integer_,
    initialize = function(seed) {
      prev.state = get0(".Random.seed", .GlobalEnv)
      prev.rng = RNGkind()[1L]
      on.exit({ RNGkind(prev.rng); assign(".Random.seed", prev.state, envir = .GlobalEnv) })
      RNGkind("L'Ecuyer-CMRG")
      set.seed(seed)
      self$start.seed = get0(".Random.seed", .GlobalEnv)

      assertInteger(self$start.seed, len = 7L, any.missing = FALSE)
      if (self$start.seed[1L] %% 100L != 7L)
        stop("Invalid value of 'seed'")
    },

    get = function(i) {
      i = asInteger(i, lower = 1, any.missing = FALSE)
      x = .Call(next_streams, self$start.seed, as.integer(max(i)))
      x[, i, drop = FALSE]
    }
  )
)

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

if (FALSE) {
  rng = RNGStream$new(123L)
  i = 1:5e6
  d = data.table(i = i, state = unname(as.list(as.data.frame(rng$get(i)))))
  print(object.size(d), unit = "Mb")
  system.time(rng$get(10000000))
}

