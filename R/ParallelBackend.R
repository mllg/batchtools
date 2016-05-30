getParallelMode = function(mode, backend, ncpus, n.jobs) {
  if (is.null(mode))
    return(list(mode = "none", backend = "sequential", ncpus = 1L))

  if (is.null(backend)) {
    backend = if(testOS("windows")) "socket" else "multicore"
  }

  if (is.null(ncpus)) {
    if (backend %in% c("multicore", "socket")) {
      loadNamespace("parallel")
      ncpus = max(parallel::detectCores(), 1L)
    } else if (backend == "mpi") {
      loadNamespace("Rmpi")
      ncpus = max(Rmpi::mpi.universe.size() - 1L, 1L)
    }
  }

  if (mode == "chunk")
    ncpus = min(n.jobs, ncpus)
  if (ncpus <= 1L)
    return(list(mode = "none", backend = "sequential", ncpus = 1L))
  return(list(mode = mode, backend = backend, ncpus = ncpus))
}

Sequential = R6Class("Sequential",
  cloneable = FALSE,
  public = list(
    spawn = function(fun, ...) list(fun(...)),
    collect = function() NULL
  )
)

Multicore = R6Class("Multicore",
  cloneable = FALSE,
  public = list(
    ncpus = NULL,

    initialize = function(ncpus) {
      loadNamespace("parallel")
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

Snow = R6Class("Snow",
  cloneable = FALSE,
  public = list(
    cl = NULL,
    avail = NULL,

    initialize = function(type, ncpus) {
      loadNamespace("snow")
      if (type == "socket") {
        self$cl = snow::makeSOCKcluster(rep.int("localhost", ncpus))
      } else {
        self$cl = snow::makeMPIcluster(ncpus)
      }
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
