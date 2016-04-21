#' @title ClusterFunctions for SLURM Systems
#'
#' @description
#' Job files are created based on the brew template \code{template.file}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{sbatch} command. Jobs are killed using the \code{scancel} command and
#' the list of running jobs is retrieved using \code{squeue}. The user must
#' have the appropriate privileges to submit, delete and list jobs on the
#' cluster (this is usually the case).
#'
#' The template file can access all arguments passed to the \code{submitJob}
#' function, see here \code{\link{ClusterFunctions}}. It is the template file's
#' job handle the desired resource allocations. The cluster selection however is
#' done via the argument \code{clusters}.
#'
#' @template template_or_text
#' @param clusters [\code{character(1)}]\cr
#'  If multiple clusters are managed by one SLURM system, the name of one cluster has to be specified.
#'  If only one cluster is present, this argument may be omitted.
#'  Note that you should not select the cluster in your template file via \code{#SBATCH --clusters}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSLURM = function(template = NULL, text = NULL, clusters = NULL) {
  if (!is.null(clusters))
    checkmate::assertString(clusters, min.chars = 1L)
  text = cfReadBrewTemplate(template, text, "##")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    if (!is.null(clusters))
      text = stri_join(text, sprintf("#SBATCH --clusters=%s", clusters), sep = "\n")
    outfile = cfBrewTemplate(reg, text, jc)
    res = runOSCommand("sbatch", outfile, stop.on.exit.code = FALSE, debug = reg$debug)

    max.jobs.msg = "sbatch: error: Batch job submission failed: Job violates accounting policy (job submit limit, user's size and/or time limits)"
    temp.error = "Socket timed out on send/recv operation"
    output = stri_join(res$output, collapse = "\n")

    if (stri_detect_fixed(max.jobs.msg, output)) {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = max.jobs.msg)
    } else if (stri_detect_fixed(temp.error, output)) {
      # another temp error we want to catch
      makeSubmitJobResult(status = 2L, batch.id = NA_character_, msg = temp.error)
    } else if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("sbatch", res$exit.code, res$output)
    } else {
      makeSubmitJobResult(status = 0L, batch.id = stri_trim_both(stri_split_fixed(output, " ")[[1L]][4L]))
    }
  }

  listJobs = function(reg, cmd) {
    cmd = c(cmd, sprintf("--clusters=%s", clusters))
    batch.ids = runOSCommand(cmd[1L], cmd[-1L], debug = reg$debug)$output

    # if cluster name is specified, the first line will be the cluster name
    if (!is.null(clusters))
      batch.ids = tail(batch.ids, -1L)

    stri_extract_first_regex(batch.ids, "[0-9]+")
  }

  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    cmd = c("squeue", "-h", "-o %i", "-u $USER", "-t PD")
    listJobs(reg, cmd)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    cmd = c("squeue", "-h", "-o %i", "-u $USER", "-t R,S,CG")
    listJobs(reg, cmd)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "scancel", c(sprintf("--clusters=%s", clusters), batch.id))
  }

  makeClusterFunctions(name = "SLURM", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    listJobsQueued = listJobsQueued, array.envir.var = "SLURM_ARRAY_TASK_ID", store.job = TRUE)
}
