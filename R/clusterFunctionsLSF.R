#' @title ClusterFunctions for LSF Systems
#'
#' @description
#' Cluster functions for LSF (\url{https://www.ibm.com/us-en/marketplace/hpc-workload-management}).
#'
#' Job files are created based on the brew template \code{template.file}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{bsub} command. Jobs are killed using the \code{bkill} command and the
#' list of running jobs is retrieved using \code{bjobs -u $USER -w}. The user
#' must have the appropriate privileges to submit, delete and list jobs on the
#' cluster (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @note
#' Array jobs are currently not supported.
#'
#' @template template
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsLSF = function(template = "lsf", scheduler.latency = 1, fs.latency = 65) { # nocov start
  template = findTemplateFile(template)
  if (testScalarNA(template))
    stopf("Argument 'template' (=\"%s\") must point to a readable template file or contain the template itself as string (containing at least one newline)", template)
  template = cfReadBrewTemplate(template)

  # When LSB_BJOBS_CONSISTENT_EXIT_CODE = Y, the bjobs command exits with 0 only
  # when unfinished jobs are found, and 255 when no jobs are found,
  # or a non-existent job ID is entered.
  Sys.setenv(LSB_BJOBS_CONSISTENT_EXIT_CODE = "Y")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("bsub", stdin = shQuote(outfile))

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("bsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_flatten(res$output, " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    res = runOSCommand("bjobs", args)
    if (res$exit.code > 0L) {
      if (res$exit.code == 255L || any(stri_detect_regex(res$output, "No (unfinished|pending|running) job found")))
        return(character(0L))
      OSError("Listing of jobs failed", res)
    }
    stri_extract_first_regex(tail(res$output, -1L), "\\d+")
  }

  listJobsQueued = function(reg) {
    listJobs(reg, c("-u $USER", "-w", "-p"))
  }

  listJobsRunning = function(reg) {
    listJobs(reg, c("-u $USER", "-w", "-r"))
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "bkill", batch.id)
  }

  makeClusterFunctions(name = "LSF", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job.collection = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
