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
      force(jc)
      if (all(nzchar(self$pids))) {
        res = snow::recvOneResult(self$cl)
        self$pids[self$pids == res$tag] = ""
      }
      i = wf(!nzchar(self$pids))
      snow::sendCall(self$cl[[i]], doJobCollection, list(jc = jc, output = jc$log.file), return = FALSE, tag = jc$job.hash)
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
#' @template ncpus
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSocket = function(ncpus = NA_integer_) {
  assertCount(ncpus, positive = TRUE, na.ok = TRUE)
  if (is.na(ncpus)) {
    ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L, na.rm = TRUE)
    info("Auto-detected %i CPUs", ncpus)
  }
  p = Socket$new(ncpus)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    p$spawn(jc)
    makeSubmitJobResult(status = 0L, batch.id = jc$job.hash)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list()
  }

  makeClusterFunctions(name = "Socket", submitJob = submitJob, listJobsRunning = listJobsRunning,
    store.job = FALSE, hooks = list(pre.sync = function(reg, fns) p$list()))
}
