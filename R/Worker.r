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
#' @name Worker
#' @rdname Worker
#' @return [\code{\link{Worker}}].
#' @export
makeWorker = function(nodename, ncpus = 0L, max.jobs = NULL, max.load = NULL) {
  assertString(nodename)
  ncpus = asInt(ncpus)

  worker = new.env(parent = emptyenv())
  worker$nodename = nodename
  worker$script = findHelperScriptLinux(nodename)
  worker$last.update = -Inf
  worker$available = "?"
  worker$status = NULL
  class(worker) = "Worker"

  if (ncpus == 0L)
    ncpus = getWorkerNumberOfCPUs(worker)
  worker$ncpus = ncpus
  worker$max.jobs = max(asInt(max.jobs %??% ncpus), 1L)
  worker$max.load = max(as.numeric(max.load %??% ncpus), 1)

  return(setClasses(worker, "Worker"))
}

getWorkerNumberOfCPUs = function(worker) {
  as.integer(runWorkerCommand(worker, "number-of-cpus")$output)
}

getWorkerStatus = function(worker, reg) {
  res = runWorkerCommand(worker, "status", reg$file.dir, debug = reg$debug)$output
  res = as.numeric(stri_split_regex(res, "\\s+")[[1L]])
  setNames(as.list(res), c("load", "n.rprocs", "n.rprocs.50", "n.jobs"))
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

findHelperScriptLinux = function(nodename) {
  if (nodename == "localhost") {
    system.file("bin", "linux-helper", package = "batchtools")
  } else {
    args = "-e \"message(system.file('bin/linux-helper', package = 'batchtools'))\""
    tail(runOSCommand("Rscript", args, nodename = nodename)$output, 1L)
  }
}

runWorkerCommand = function(worker, command, args = character(0L), debug = FALSE) {
  script.args = c(command, args)
  runOSCommand(worker$script, script.args, nodename = worker$nodename, debug = debug)
}

# is a worker busy, see rules below
getWorkerSchedulerStatus = function(worker) {
  # we have already used up our maximal jobs on this node
  if (worker$status$n.jobs >= worker$max.jobs)
    return("max.jobs")
  # should not have too much load average
  if (worker$status$load[1L] >= worker$max.load)
    return("max.load")
  # there are already ncpus expensive R jobs running on the node
  if (worker$status$n.rprocs.50 >= 2*worker$ncpus)
    return("max.r50")
  # should not have too many R sessions open
  if(worker$status$n.rprocs >= 3 * worker$ncpus)
    return("max.r")
  # else all clear, submit the job!
  return("avail")
}

updateWorker = function(worker, reg, tdiff = 0L) {
  time = now()
  if (worker$available == "avail" || time - worker$last.update >= tdiff) {
    worker$last.update = time
    worker$status = getWorkerStatus(worker, reg)
    worker$available = getWorkerSchedulerStatus(worker)
  }
}

findWorker = function(workers, reg, tdiff = 0L) {
  lapply(workers, updateWorker, reg = reg, tdiff = tdiff)
  rload = vnapply(workers, function(w) w$status$load / w$ncpus)
  Find(function(w) w$available == "avail", sample(workers, prob = 1 / (rload + 0.1)), nomatch = NULL)
}
