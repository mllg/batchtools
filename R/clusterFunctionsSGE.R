#' @title ClusterFunctions for SGE Systems
#'
#' @description
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
#' @templateVar cf.name sge
#' @template template
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSGE = function(template = findTemplateFile("sge")) { # nocov start
  template = cfReadBrewTemplate(template)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", outfile)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("qsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_join(res$output, collapse = " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, cmd) {
    batch.ids = runOSCommand(cmd[1L], cmd[-1L])$output
    stri_extract_first_regex(tail(batch.ids, -2L), "\\d+")
  }

  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("qstat",  "-u $USER", "-s p"))
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("qstat",  "-u $USER", "-s rs"))
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "qdel", batch.id)
  }

  makeClusterFunctions(name = "SGE", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, store.job = TRUE, array.var = "SGE_TASK_ID")
} # nocov end
