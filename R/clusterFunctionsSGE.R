#' @title ClusterFunctions for SGE Systems
#'
#' @description
#' Cluster functions for Univa Grid Engine / Oracle Grid Engine /
#' Sun Grid Engine (\url{http://www.univa.com/}).
#'
#' Job files are created based on the brew template \code{template}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{qsub} command. Jobs are killed using the \code{qdel} command and the
#' list of running jobs is retrieved using \code{qselect}. The user must have
#' the appropriate privileges to submit, delete and list jobs on the cluster
#' (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @note
#' Array jobs are currently not supported.
#'
#' @templateVar cf.name sge
#' @template template
#' @inheritParams makeClusterFunctions
#' @template nodename
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSGE = function(template = "sge", nodename = "localhost", scheduler.latency = 1, fs.latency = 65) { # nocov start
  assertString(nodename)
  template = findTemplateFile(template)
  template = cfReadBrewTemplate(template)
  quote = if (isLocalHost(nodename)) identity else shQuote

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", outfile, nodename = nodename)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("qsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_flatten(res$output, " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    res = runOSCommand("qstat", args, nodename = nodename)
    if (res$exit.code > 0L)
      OSError("Listing of jobs failed", res)
    stri_extract_first_regex(tail(res$output, -2L), "\\d+")
  }

  listJobsQueued = function(reg) {
    listJobs(reg, c(quote("-u $USER"), "-s p"))
  }

  listJobsRunning = function(reg) {
    listJobs(reg, c(quote("-u $USER"), "-s rs"))
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "qdel", batch.id, nodename = nodename)
  }

  makeClusterFunctions(name = "SGE", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job.collection = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
