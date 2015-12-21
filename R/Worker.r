#' @title Abstract base class constructor for general workers
#'
#' @description
#' Functions to construct local and remote workers.
#'
#' @param nodename [\code{character(1)}]\cr
#'   Host name of node.
#' @param ncpus [\code{integers(1)}]\cr
#'   Number of VPUs of worker. Default (0) means to query the worker.
#' @param max.jobs [\code{integer(1)}]\cr
#'   Maximal number of jobs that can run concurrently for the current registry.
#'   Default is \code{ncpus}.
#' @param max.load [\code{numeric(1)}]\cr
#'   Load average (of the last 5 min) at which the worker is considered
#'   occupied, so that no job can be submitted.
#'   Default is \code{ncpus}.
#' @param debug [\code{logical(1)}]\cr
#'   Print some verbose info messages. Default is \code{FALSE}.
#' @name Worker
#' @rdname Worker
#' @return [\code{\link{Worker}}].
#' @export
#' @examples
#' \dontrun{
#' # create a worker for the local machine and use 4 CPUs.
#' makeWorker("localhost", ncpus = 4, max.jobs = 4)
#' }
makeWorker = function(nodename, ncpus = 0L, max.jobs = NULL, max.load = NULL, debug = FALSE) {
  findHelperScriptLinux = function(nodename) {
    if (nodename == "localhost") {
      system.file("bin", "linux-helper", package = "batchtools")
    } else {
      args = c("-e", shQuote("message(system.file('bin/linux-helper', package = 'batchtools'))"))
      tail(runOSCommand("Rscript", args, nodename = nodename, debug = debug)$output, 1L)
    }
  }

  assertString(nodename)
  if (!is.null(max.jobs))
    assertIntegerish(max.jobs)
  if (!is.null(max.load))
    assertNumeric(max.load)
  ncpus = asInt(ncpus)
  assertFlag(debug)

  worker = new.env(parent = emptyenv())
  worker$nodename = nodename
  worker$script = findHelperScriptLinux(nodename)
  worker$last.update = -Inf
  worker$available = "unknown"
  worker$status = NULL
  class(worker) = "Worker"

  if (ncpus == 0L)
    ncpus = as.integer(runWorkerCommand(worker, "number-of-cpus", debug = debug)$output)
  worker$ncpus = ncpus
  worker$max.jobs = max(as.integer(max.jobs %??% ncpus), 1L)
  worker$max.load = max(max.load %??% ncpus, 1)

  return(setClasses(worker, "Worker"))
}

getWorkerStatus = function(worker, reg) {
  res = runWorkerCommand(worker, "status", reg$file.dir, debug = reg$debug)$output
  res = as.list(as.numeric(stri_split_regex(res, "\\s+")[[1L]]))
  names(res) = c("load", "n.rprocs", "n.rprocs.50", "n.jobs")
  return(res)
}

startWorkerJob = function(worker, reg, job, outfile) {
  runWorkerCommand(worker, "start-job", c(job, outfile), debug = reg$debug)$output
}

killWorkerJob = function(worker, reg, pid) {
  runWorkerCommand(worker, "kill-job", pid, debug = reg$debug)$exit.code == 0L
}

listWorkerJobs = function(worker, reg) {
  stri_trim_both(runWorkerCommand(worker, "list-jobs", reg$file.dir, debug = reg$debug)$output)
}

runWorkerCommand = function(worker, command, args = character(0L), debug = FALSE) {
  script.args = c(command, args)
  runOSCommand(worker$script, script.args, nodename = worker$nodename, debug = debug)
}

updateWorker = function(worker, reg) {
  time = now()
  if (worker$available != "avail") {
    worker$last.update = time
    worker$status = getWorkerStatus(worker, reg)
    if (worker$status$n.jobs >= worker$max.jobs) {
      worker$available = "max.jobs"
    } else if (worker$status$load[1L] >= worker$max.load) {
      worker$available = "max.load"
    } else {
      worker$available = "avail"
    }
  }
  worker
}
