if (getRversion() < "3.3.2" && .Platform$OS.type != "windows") {
  # Provided patch for upstream which is shipped with R >= 3.3.2:
  # https://stat.ethz.ch/pipermail/r-devel/2016-August/073035.html
  selectChildren = getFromNamespace("selectChildren", "parallel")
  readChild = getFromNamespace("readChild", "parallel")

  mccollect = function(pids, timeout = 0) {
    if (!length(pids)) return (NULL)
    if (!is.integer(pids)) stop("invalid 'jobs' argument")

    s = selectChildren(pids, timeout)
    if (is.logical(s) || !length(s)) return(NULL)
    res = lapply(s, function(x) {
      r = readChild(x)
      if (is.raw(r)) unserialize(r) else NULL
    })
    names(res) = as.character(pids)[match(s, pids)]
    res
  }
} else {
  mccollect = function(jobs, timeout = 0) {
    parallel::mccollect(jobs, wait = FALSE, timeout = timeout)
  }
}

Multicore = R6Class("Multicore",
  cloneable = FALSE,
  public = list(
    jobs = NULL,
    ncpus = NA_integer_,

    initialize = function(ncpus) {
      self$jobs = data.table(pid = integer(0L), count = integer(0L))
      self$ncpus = ncpus
      reg.finalizer(self, function(e) mccollect(self$jobs$pid, timeout = 1), onexit = FALSE)
    },

    spawn = function(jc) {
      force(jc)
      repeat {
        self$collect(0)
        if (nrow(self$jobs) < self$ncpus)
          break
        Sys.sleep(1)
      }
      pid = parallel::mcparallel(doJobCollection(jc, output = jc$log.file), mc.set.seed = FALSE)$pid
      self$jobs = rbind(self$jobs, data.table(pid = pid, count = 0L))
      invisible(as.character(pid))
    },

    list = function() {
      self$collect(0)
      as.character(self$jobs$pid)
    },

    collect = function(timeout) {
      repeat {
        res = mccollect(self$jobs$pid, timeout = timeout)
        if (is.null(res))
          break
        pids = as.integer(names(res))
        self$jobs[pid %in% pids, count := count + 1L]
        self$jobs = self$jobs[count < 1L]
      }
    }
  )
)

#' @title ClusterFunctions for Parallel Multicore Execution
#'
#' @description
#' Jobs are spawned asynchronously using the functions \code{mcparallel} and \code{mccollect} (both in \pkg{parallel}).
#' Does not work on Windows, use \code{\link{makeClusterFunctionsSocket}} instead.
#'
#' @template ncpus
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsMulticore = function(ncpus = NA_integer_, fs.latency = 0) {
  if (testOS("windows"))
    stop("ClusterFunctionsMulticore do not support Windows. Use makeClusterFunctionsSocket instead.")
  if (is.na(ncpus)) {
    ncpus = max(as.numeric(getOption("mc.cores")), parallel::detectCores(), 1L, na.rm = TRUE)
    info("Auto-detected %i CPUs", ncpus)
  }
  ncpus = asCount(ncpus, na.ok = FALSE, positive = TRUE)
  p = Multicore$new(ncpus)

  submitJob = function(reg, jc) {
    force(jc)
    pid = p$spawn(jc)
    makeSubmitJobResult(status = 0L, batch.id = pid)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list()
  }

  makeClusterFunctions(name = "Multicore", submitJob = submitJob, listJobsRunning = listJobsRunning,
    store.job.collection = FALSE, fs.latency = fs.latency, hooks = list(pre.sync = function(reg, fns) p$collect(1)))
}
