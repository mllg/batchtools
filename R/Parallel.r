Sequential = R6Class("Sequential",
  public = list(
    spawn = function(expr) return(list(eval(expr))),
    collect = function() NULL
  )
)

Parallel = R6Class("Parallel",
  public = list(
    ncpus = NA_integer_,

    initialize = function(ncpus) {
      self$ncpus = ncpus
      # reg.finalizer(self, function(e) mccollect(wait = TRUE), onexit = TRUE)
    },

    spawn = function(expr) {
      if (self$ncpus >= 1L) {
        self$ncpus = self$ncpus - 1L
        parallel::mcparallel(expr, mc.set.seed = FALSE)
        return(list())
      }

      results = list()
      while(length(results) == 0L) {
        results = filterNull(unname(parallel::mccollect(wait = FALSE, timeout = 1)))
      }

      parallel::mcparallel(expr)
      self$ncpus = self$ncpus + length(results) - 1L
      return(results)
    },

    collect = function() {
      filterNull(unname(parallel::mccollect(wait = TRUE)))
    }
  )
)
