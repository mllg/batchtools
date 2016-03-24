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
#' The template file can access all arguments passed to the \code{submitJob}
#' function, see here \code{\link{ClusterFunctions}}. It is the template file's
#' job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @template template
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSGE = function(template) {
  list.jobs.cmd = c("qstat",  "-u $USER")
  template = cfReadBrewTemplate(template)

  submitJob = function(reg, jc) {
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("qsub", outfile, stop.on.exit.code = FALSE, debug = reg$debug)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("qsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_join(res$output, collapse = " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, cmd) {
    batch.ids = runOSCommand(cmd[1L], cmd[-1L], debug = reg$debug)$output
    stri_extract_first_regex(tail(batch.ids, -2L), "\\d+")
  }

  listJobsQueued = function(reg) {
    listJobs(reg, c(list.jobs.cmd, "-s p"))
  }

  listJobsRunning = function(reg) {
    listJobs(reg, c(list.jobs.cmd, "-s rs"))
  }

  killJob = function(reg, batch.id) {
    cfKillBatchJob("qdel", batch.id)
  }

  makeClusterFunctions(name = "SGE", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
    listJobsRunning = listJobsRunning, array.envir.var = "SGE_TASK_ID", store.job = TRUE)
}
