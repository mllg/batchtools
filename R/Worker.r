#' @title Abstract base class constructor for general workers
#'
#' @description
#' Functions to construct local and remote workers.
#'
#' @param nodename [\code{character(1)}]\cr
#'   Host name of node.
#' @param ncpus [\code{integers(1)}]\cr
#'   Number of VPUs of worker.
#'   Default (0) means to query the worker via \code{\link{getWorkerNumberOfCPUs}}.
#' @param max.jobs [\code{integer(1)}]\cr
#'   Maximal number of jobs that can run concurrently for the current registry.
#'   Default is \code{ncpus}.
#' @param max.load [\code{numeric(1)}]\cr
#'   Load average (of the last 5 min) at which the worker is considered occupied,
#'   so that no job can be submitted.
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
  worker$available = "A"
  worker$status = NULL
  class(worker) = "Worker"

  if (ncpus == 0L)
    ncpus = getWorkerNumberOfCPUs(worker)
  worker$ncpus = ncpus
  worker$max.jobs = asInt(max.jobs %??% ncpus)
  worker$max.load = asInt(max.load %??% ncpus)

  return(setClasses(worker, "Worker"))
}

getWorkerNumberOfCPUs = function(worker) {
  as.integer(runWorkerCommand(worker, "number-of-cpus"))
}

getWorkerStatus = function(worker, file.dir) {
  res = runWorkerCommand(worker, "status", file.dir)
  res = as.numeric(stri_split_regex(res, "\\s+")[[1L]])
  setNames(as.list(res), c("load", "n.rprocs", "n.rprocs.50", "n.jobs"))
}

startWorkerJob = function(worker, job, outfile) {
  runWorkerCommand(worker, "start-job", c(job, outfile))
}

killWorkerJob = function(worker, pid) {
  runWorkerCommand(worker, "kill-job", pid)
}

listWorkerJobs = function(worker, file.dir) {
  stri_trim_both(runWorkerCommand(worker, "list-jobs", file.dir))
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
  runOSCommand(worker$script, script.args, nodename = worker$nodename, debug = debug)$output
}

# is a worker busy, see rules below
getWorkerSchedulerStatus = function(worker) {
  # we have already used up our maximal jobs on this node
  if (worker$status$n.jobs >= worker$max.jobs)
    return("J")
  # should not have too much load average
  if (worker$status$load[1L] > worker$max.load)
    return("L")
  # there are already ncpus expensive R jobs running on the node
  if (worker$status$n.rprocs.50 >= worker$ncpus)
    return("R")
  # should not have too many R sessions open
  if(worker$status$n.rprocs >= 3 * worker$ncpus)
    return("r")
  # else all clear, submit the job!
  return("A")
}

updateWorker = function(worker, file.dir, tdiff) {
  time = now()
  if (worker$available == "A" || time - worker$last.update >= tdiff) {
    worker$last.update = time
    worker$status = getWorkerStatus(worker, file.dir)
    worker$available = getWorkerSchedulerStatus(worker)
  }
}

# find worker via isBusyWorker and update workers while looking
# workers with a low load are more likely to be selected when there are
# multiple workers available
findWorker = function(workers, file.dir, tdiff) {
  lapply(workers, updateWorker, file.dir = file.dir, tdiff = tdiff)
  rload = vnapply(workers, function(w) w$status$load / w$ncpus)
  Find(function(w) w$available == "A", sample(workers, prob = 1 / (rload + 0.1)), nomatch = NULL)
}

if (FALSE) {
  reg = makeTempRegistry(TRUE)
  reg$cluster.functions = makeClusterFunctionsMulticore(1, ncpus = 2, max.load = 1000)
  reg$debug = TRUE
  saveRegistry(reg)
  batchMap(Sys.sleep, 1:10)
  submitJobs()
  getStatus()

  worker = makeWorker("localhost", ncpus = 0L)
  outfile = jc$log.file
  job = jc$uri
  saveRDS(jc, file = job)
}
