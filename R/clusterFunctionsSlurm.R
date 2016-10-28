#' @title ClusterFunctions for Slurm Systems
#'
#' @description
#' Job files are created based on the brew template \code{template.file}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{sbatch} command. Jobs are killed using the \code{scancel} command and
#' the list of running jobs is retrieved using \code{squeue}. The user must
#' have the appropriate privileges to submit, delete and list jobs on the
#' cluster (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' Note that you might have to specify the cluster name here if you do not want to use the default,
#' otherwise the commands for listing and killing jobs will not work.
#'
#' @templateVar cf.name slurm
#' @template template
#' @param clusters [\code{character(1)}]\cr
#'  If multiple clusters are managed by one Slurm system, the name of one cluster has to be specified.
#'  If only one cluster is present, this argument may be omitted.
#'  Note that you should not select the cluster in your template file via \code{#SBATCH --clusters}.
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSlurm = function(template = findTemplateFile("slurm"), clusters = NULL) { # nocov start
  if (!is.null(clusters))
    assertString(clusters, min.chars = 1L)
  template = cfReadBrewTemplate(template, "##")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    jc$clusters = clusters
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("sbatch", outfile)

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
      makeSubmitJobResult(status = 0L, batch.id = stri_trim_both(stri_split_fixed(output[1L], " ")[[1L]][4L]))
    }
  }

  listJobs = function(reg, cmd) {
    cmd = c(cmd, sprintf("--clusters=%s", clusters))
    batch.ids = runOSCommand(cmd[1L], cmd[-1L])$output

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

  makeClusterFunctions(name = "Slurm", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    listJobsQueued = listJobsQueued, store.job = TRUE, array.var = "SLURM_ARRAY_TASK_ID")
} # nocov end
