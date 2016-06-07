Socket = R6Class("Socket",
  cloneable = FALSE,
  public = list(
    cl = NULL,
    pids = NULL,

    initialize = function(ncpus) {
      loadNamespace("snow")
      self$cl = snow::makeSOCKcluster(rep.int("localhost", ncpus))
      self$pids = character(ncpus)
      reg.finalizer(self, function(e) if (!is.null(e$cl)) { snow::stopCluster(e$cl); self$cl = NULL }, onexit = TRUE)
    },

    spawn = function(jc, ...) {
      if (all(nzchar(self$pids))) {
        res = snow::recvOneResult(self$cl)
        self$pids[self$pids == res$tag] = ""
      }
      i = wf(!nzchar(self$pids))
      snow::sendCall(self$cl[[i]], doJobCollection, list(jc = jc, con = jc$log.file), return = FALSE, tag = jc$job.hash)
      self$pids[i] = jc$job.hash
      invisible(jc$job.hash)
    },

    list = function() {
      if (is.null(self$cl))
        return(character(0L))

      sl = lapply(self$cl, function(x) x$con)
      finished = which(socketSelect(sl, write = FALSE, timeout = 1))
      for (i in seq_along(finished)) {
        res = snow::recvOneResult(self$cl)
        self$pids[self$pids == res$tag] = ""
      }

      self$pids[nzchar(self$pids)]
    }
  )
)

#' @title ClusterFunctions for Parallel Socket Execution
#'
#' @description
#' Jobs are spawned asynchronously using the package \pkg{snow}.
#'
#' @param ncpus [\code{integer(1)}]\cr
#'   Number of VPUs of worker.
#'   Default is to use all cores but one, where total number of cores "available" is given by option \code{mc.cores}
#'   and defaults to the heuristic implemented in \code{\link[parallel]{detectCores}}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family clusterFunctions
#' @export
makeClusterFunctionsSocket = function(ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L)) {
  assertCount(ncpus, positive = TRUE)
  p = Socket$new(ncpus)

  submitJob = function(reg, jc) {
    p$spawn(jc)
    makeSubmitJobResult(status = 0L, batch.id = jc$job.hash, msg = "")
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list()
  }

  makeClusterFunctions(name = "Socket", submitJob = submitJob, listJobsRunning = listJobsRunning,
    hooks = list(pre.sync = function(reg, fns) p$list()), store.job = FALSE)
}
