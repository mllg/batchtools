#' @title ClusterFunctions for OpenLava
#'
#' @description
#' Job files are created based on the brew template \code{template}. This
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
#' @templateVar cf.name openlava
#' @template template
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsOpenLava = function(template = findTemplateFile("openlava")) { # nocov start
  template = cfReadBrewTemplate(template)

  # When LSB_BJOBS_CONSISTENT_EXIT_CODE = Y, the bjobs command exits with 0 only
  # when unfinished jobs are found, and 255 when no jobs are found,
  # or a non-existent job ID is entered.
  Sys.setenv(LSB_BJOBS_CONSISTENT_EXIT_CODE = "Y")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("bsub", outfile)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("bsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_join(res$output, collapse = " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, cmd) {
    res = runOSCommand(cmd[1L], cmd[-1L])$output
    if (res$exit.code == 255L && stri_detect_fixed(res$output, "No unfinished job found"))
      return(character(0L))
    if (res$exit.code > 0L)
      stopf("Command '%s' produced exit code: %i; output: %s", stri_join(cmd, collapse = " "), res$exit.code, res$output)

    stri_extract_first_regex(tail(res$output, -1L), "\\d+")
  }

  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("bjobs", "-u $USER", "-w", "-p"))
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("bjobs", "-u $USER", "-w", "-r"))
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "bkill", batch.id)
  }

  makeClusterFunctions(name = "OpenLava", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job = TRUE, array.var = "LSB_JOBINDEX")
} # nocov end
