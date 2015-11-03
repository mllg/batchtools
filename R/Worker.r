#' @title Abstract base class constructor for general workers
#'
#' @description
#' Functions to construct local and remote workers.
#'
#' @param nodename [\code{character(1)}]\cr
#'   Host name of node.
#' @param script [\code{character(1)}]\cr
#'   Path to helper script on worker.
#'   Default means to call \code{\link{findHelperScriptLinux}}.
#' @param ncpus [\code{integers(1)}]\cr
#'   Number of VPUs of worker.
#'   Default means to query the worker via \code{\link{getWorkerNumberOfCPUs}}.
#' @param max.jobs [\code{integer(1)}]\cr
#'   Maximal number of jobs that can run concurrently for the current registry.
#'   Default is \code{ncpus}.
#' @param max.load [\code{numeric(1)}]\cr
#'   Load average (of the last 5 min) at which the worker is considered occupied,
#'   so that no job can be submitted.
#'   Default is \code{ncpus-1}.
#' @param nice [\code{integer(1)}]\cr
#'   Process priority to run R with set via nice. Integers between -20 and 19 are allowed.
#'   If missing, processes are not nice'd and the system default applies (usually 0).
#' @name Worker
#' @rdname Worker
#' @return [\code{\link{Worker}}].
#' @export
makeWorker = function(nodename, ncpus, max.jobs, max.load, nice) {
  assertString(nodename)
  ncpus = asInt(ncpus)
  max.jobs = asInt(max.jobs)
  max.load = asInt(max.load)

  worker = list2env(list(
    nodename = nodename,
    ncpus = asInt(ncpus),
    max.jobs = asInt(max.jobs),
    max.load = asInt(max.load),
    script = findHelperScriptLinux(nodename),
    last.update = -Inf,
    available = "A",
    status = NULL
  ), parent = emptyenv())

  return(setClasses(worker, "Worker"))
}

# Return number of cores on worker.
# @param worker [\code{\link{Worker}}].
#   Worker.
# @return [\code{integer(1)}].
getWorkerNumberOfCPUs = function(worker) {
  as.integer(runWorkerCommand(worker, "number-of-cpus"))
}

# Return 4 numbers to describe worker status.
# - load average of last 1 min, as given by e.g. uptime
# - number of R processes by _all_ users
# - number of R processes by _all_ users which have a load of >= 50%
# - number of R processes by current user which match $FILEDIR/jobs in the cmd call of R
# @param worker [\code{\link{Worker}}].
#   Worker.
# @param file.dir [\code{character(1)}}].
#   File dir of registry.
# @return [named \code{list} of \code{numeric(1)}].
getWorkerStatus = function(worker, file.dir) {
  res = runWorkerCommand(worker, "status", file.dir)
  res = as.numeric(stri_split_regex(res, "\\s+")[[1L]])
  setNames(as.list(res), c("load", "n.rprocs", "n.rprocs.50", "n.jobs"))
}

# Start a job on worker, probably with R CMD BATCH.
# @param worker [\code{\link{Worker}}].
#   Worker.
# @param rfile [\code{character(1)}].
#   Path to R file to execute.
# @param outfile [\code{character(1)}].
#   Path to log file for R process.
# @return [\code{character(1)}]. Relevant process id.
startWorkerJob = function(worker, rfile, outfile) {
  runWorkerCommand(worker, "start-job", c(worker$nice, rfile, outfile))
}

# Kill a job on worker. Really do it.
# @param worker [\code{\link{Worker}}].
#   Worker.
# @param pid [\code{character(1)}].
#   Process id from DB/batch.job.id to kill.
# @return Nothing.
killWorkerJob = function(worker, pid) {
  runWorkerCommand(worker, "kill-job", pid)
}

# List all jobs on worker belonging to the current registry.
# @param worker [\code{\link{Worker}}].
#   Worker.
# @param file.dir [\code{character(1)}}].
#   File dir of registry.
# @return [\code{character}]. Vector of process ids.
listWorkerJobs = function(worker, file.dir) {
  res = runWorkerCommand(worker, "list-jobs", file.dir)
  stri_trim_both(res)
}

findHelperScriptLinux = function(nodename) {
  args = "-e \"message(normalizePath(system.file('bin/linux-helper', package = 'batchtools')))\""
  tail(runOSCommand("Rscript", args, nodename = nodename)$output, 1L)
}

runWorkerCommand = function(worker, command, args = character(0L)) {
  script.args = c(command, args)
  runOSCommand(worker$script, script.args, nodename = worker$nodename)$output
}
