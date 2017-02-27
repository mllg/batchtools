RNG = R6Class("RNG",
  cloneable = FALSE,
  public = list(
    states = NULL,

    initialize = function(start, i) {
      self$setRNG()
      private$compute(start, i)
      self$nextStream()
    },

    setRNG = function() {
      current = RNGkind()
      if (!exists(".Random.seed", .GlobalEnv))
        set.seed(NULL)
      private$prev$state = get0(".Random.seed", envir = .GlobalEnv)

      if (current[1L] != self$kind) {
        "!DEBUG [RNG] Setting RNG to `self$kind`"
        private$prev$kind = current[1L]
        private$prev$normal.kind = current[2L]
        RNGkind(self$kind)
      }
    },

    restore = function() {
      if (!is.null(private$prev$kind)) {
        "!DEBUG [RNG] Resetting RNG to `private$prev$kind`"
        RNGkind(private$prev$kind, private$prev$normal.kind)
      }

      if (!is.null(private$prev$state)) {
        "!DEBUG [RNG] Restored previous state"
        assign(".Random.seed", private$prev$state, envir = .GlobalEnv)
      }
    }
  ),

  private = list(
    prev = list(),
    i = 0L
  )
)

#' @useDynLib batchtools next_streams
RNGLecuyer = R6Class("RNG",
  cloneable = FALSE,
  inherit = RNG,
  public = list(
    kind = "L'Ecuyer-CMRG",
    nextStream = function() {
      private$i = private$i + 1L
      if (private$i > ncol(self$states))
        stop("No more RNG Streams remaining")
      assign(".Random.seed", self$states[, private$i], envir = .GlobalEnv)
    }
  ),

  private = list(
    compute = function(start, i) {
      set.seed(start, kind = self$kind)
      start = get0(".Random.seed", envir = .GlobalEnv)
      self$states = .Call(next_streams, start, i)
    }
  )
)


RNGMersenne = R6Class("RNGMersenne",
  cloneable = FALSE,
  inherit = RNG,
  public = list(
    kind = "Mersenne-Twister",
    nextStream = function() {
      private$i = private$i + 1L
      if (private$i > length(self$states))
        stop("No more RNG Streams remaining")
      set.seed(self$states[private$i])
    }
  ),

  private = list(
    compute = function(start, i) {
      self$states = ifelse(i > .Machine$integer.max - start, start - .Machine$integer.max + i, start + i)
    }
  )
)

getRNG = function(kind, seed, i) {
  seed = asCount(seed)
  i = asInteger(i, any.missing = FALSE, lower = 1L)
  switch(kind,
    "mersenne" = RNGMersenne$new(seed, i),
    "lecuyer" = RNGLecuyer$new(seed, i),
    stop("Invalid value for RNG kind")
  )
}

if (FALSE) {
  getRNG("mersenne", 123, 2)
  rng = RNGLecuyer$new(1L, 1:3)
  .Random.seed
  rng$nextStream()
  .Random.seed
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
