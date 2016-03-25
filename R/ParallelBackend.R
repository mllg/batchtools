Sequential = R6Class("Sequential",
  cloneable = FALSE,
  public = list(
    spawn = function(fun, ...) list(fun(...)),
    collect = function() NULL
  )
)

Parallel = R6Class("Parallel",
  cloneable = FALSE,
  public = list(
    ncpus = NULL,

    initialize = function(ncpus) {
      requireNamespace("parallel")
      self$ncpus = ncpus
      # reg.finalizer(self, function(e) mccollect(wait = TRUE), onexit = TRUE)
    },

    spawn = function(fun, ...) {
      if (self$ncpus >= 1L) {
        self$ncpus = self$ncpus - 1L
        parallel::mcparallel(fun(...), mc.set.seed = FALSE)
        return(list())
      }

      results = list()
      while(length(results) == 0L)
        results = filterNull(unname(parallel::mccollect(wait = FALSE, timeout = 1)))

      parallel::mcparallel(fun(...))
      self$ncpus = self$ncpus + length(results) - 1L
      return(results)
    },

    collect = function() {
      filterNull(unname(parallel::mccollect(wait = TRUE)))
    }
  )
)

Snow = R6Class("ParallelSnow",
  cloneable = FALSE,
  public = list(
    cl = NULL,
    avail = NULL,

    initialize = function(ncpus) {
      requireNamespace("snow")
      self$cl = snow::makeSOCKcluster(rep.int("localhost", ncpus))
      self$avail = rep.int(TRUE, ncpus)
      reg.finalizer(self, function(e) if (!is.null(e$cl)) snow::stopCluster(e$cl), onexit = TRUE)
    },

    spawn = function(fun, ...) {
      i = wf(self$avail)

      if (length(i) == 0L) {
        res = snow::recvOneData(self$cl)
        i = res$node
        res = list(res$value$value)
      } else {
        res = NULL
      }
      snow::sendCall(self$cl[[i]], fun, list(...), return = FALSE)
      self$avail[i] = FALSE
      return(res)
    },

    collect = function() {
      res = lapply(seq_len(sum(!self$avail)), function(i) snow::recvOneData(self$cl)$value$value)
      if (!is.null(self$cl)) {
        snow::stopCluster(self$cl)
        self$cl = NULL
      }
      return(res)
    }
  )
)
