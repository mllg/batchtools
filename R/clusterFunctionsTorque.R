#' @title ClusterFunctions for OpenPBS/Torque Systems
#'
#' @description
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
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsTorque = function(template = findTemplateFile("torque")) { # nocov start
  template = cfReadBrewTemplate(template, "##")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", outfile)

    max.jobs.msg = "Maximum number of jobs already in queue"
    output = stri_join(res$output, collapse = "\n")

    if (stri_detect_fixed(max.jobs.msg, output)) {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = max.jobs.msg)
    } else if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("qsub", res$exit.code, res$output)
    } else {
      makeSubmitJobResult(status = 0L, batch.id = stri_trim_both(output))
    }
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "qdel", batch.id)
  }

  listJobs = function(reg, cmd) {
    batch.ids = runOSCommand(cmd[1L], cmd[-1L])$output
    unique(stri_replace_all_regex(batch.ids, "\\[[0-9]+\\]", "[]"))
  }

  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("qselect", "-u $USER", "-s QW"))
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("qselect", "-u $USER", "-s EHRT"))
  }

  makeClusterFunctions(name = "Torque", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job = TRUE, array.var = "PBS_ARRAYID")
} # nocov end
