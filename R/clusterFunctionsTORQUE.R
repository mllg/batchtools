#' @title ClusterFunctions for OpenPBS/TORQUE Systems
#'
#' @description
#' Cluster functions for TORQUE/PBS (\url{http://www.adaptivecomputing.com/products/open-source/torque/}).
#'
#' Job files are created based on the brew template \code{template.file}. This file is processed
#' with brew and then submitted to the queue using the \code{qsub} command. Jobs are killed using
#' the \code{qdel} command and the list of running jobs is retrieved using \code{qselect}. The user
#' must have the appropriate privileges to submit, delete and list jobs on the cluster (this is
#' usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @templateVar cf.name torque
#' @template template
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsTORQUE = function(template = "torque", scheduler.latency = 1, fs.latency = 65) { # nocov start
  template = findTemplateFile(template)
  template = cfReadBrewTemplate(template, "##")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", shQuote(outfile))
    output = stri_flatten(stri_trim_both(res$output), "\n")

    if (res$exit.code > 0L) {
      max.jobs.msg = "Maximum number of jobs already in queue"
      if (stri_detect_fixed(output, max.jobs.msg) || res$exit.code == 228L)
        return(makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = max.jobs.msg))
      return(cfHandleUnknownSubmitError("qsub", res$exit.code, res$output))
    }

    if (jc$array.jobs) {
      logs = sprintf("%s-%i", fs::path_file(jc$log.file), seq_row(jc$jobs))
      makeSubmitJobResult(status = 0L, batch.id = stri_replace_first_fixed(output, "[]", stri_paste("[", seq_row(jc$jobs), "]")), log.file = logs)
    } else {
      makeSubmitJobResult(status = 0L, batch.id = output)
    }
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "qdel", batch.id)
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    res = runOSCommand("qselect", args)
    if (res$exit.code > 0L)
      OSError("Listing of jobs failed", res)
    res$output
  }

  listJobsQueued = function(reg) {
    args = c("-u $USER", "-s QW")
    listJobs(reg, args)
  }

  listJobsRunning = function(reg) {
    args = c("-u $USER", "-s EHRT")
    listJobs(reg, args)
  }

  makeClusterFunctions(name = "TORQUE", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, array.var = "PBS_ARRAYID", store.job.collection = TRUE,
    scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
