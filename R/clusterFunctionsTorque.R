#' @title ClusterFunctions for OpenPBS/Torque Systems
#'
#' @description
#' Job files are created based on the brew template \code{template.file}. This file is processed
#' with brew and then submitted to the queue using the \code{qsub} command. Jobs are killed using
#' the \code{qdel} command and the list of running jobs is retrieved using \code{qselect}. The user
#' must have the appropriate privileges to submit, delete and list jobs on the cluster (this is
#' usually the case).
#'
#' The template file can access all arguments passed to the \code{submitJob} function, see here
#' \code{\link{ClusterFunctions}}. It is the template file's job to choose a queue for the job and
#' handle the desired resource allocations.
#'
#' @template template
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsTorque = function(template) {
  list.jobs.cmd = c("qselect", "-u $USER")
  template = cfReadBrewTemplate(template, "##")

  submitJob = function(reg, jc) {
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", outfile, stop.on.exit.code = FALSE, debug = reg$debug)

    max.jobs.msg = "Maximum number of jobs already in queue"
    output = paste0(res$output, collapse = "\n")

    if (stri_detect_fixed(max.jobs.msg, output)) {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = max.jobs.msg)
    } else if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("qsub", res$exit.code, res$output)
    } else {
      makeSubmitJobResult(status = 0L, batch.id = stri_trim_both(output))
    }
  }

  killJob = function(reg, batch.id) {
    cfKillBatchJob("qdel", batch.id)
  }

  listJobs = function(reg, cmd) {
    batch.ids = runOSCommand(cmd[1L], cmd[-1L], debug = reg$debug)$output
    unique(stri_replace_all_regex(batch.ids, "\\[[0-9]+\\]", "[]"))
  }

  listJobsQueued = function(reg) {
    listJobs(reg, c(list.jobs.cmd, "-s QW"))
  }

  listJobsRunning = function(reg) {
    listJobs(reg, c(list.jobs.cmd, "EHRT"))
  }

  makeClusterFunctions(name = "Torque", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    listJobsQueued = listJobsQueued, array.envir.var = "PBS_ARRAYID", store.job = TRUE)
}
