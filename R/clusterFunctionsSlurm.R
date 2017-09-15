#' @title ClusterFunctions for Slurm Systems
#'
#' @description
#' Cluster functions for Slurm (\url{http://slurm.schedmd.com/}).
#'
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
#' @param array.jobs [\code{logical(1)}]\cr
#'  If array jobs are disabled on the computing site, set to \code{FALSE}.
#' @param nodename [\code{character(1)}]\cr
#'  Nodename of the master. All commands are send via SSH to this host. Only works iff
#'  \enumerate{
#'    \item{Passwordless authentication (e.g., via SSH public key authentication) is set up.}
#'    \item{The file directory is shared across machines, e.g. mounted via SSHFS.}
#'    \item{The absolute path to the \code{file.dir} are identical on the machines, or paths are provided relative to the
#'      home directory.}
#'  }
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSlurm = function(template = "slurm", clusters = NULL, array.jobs = TRUE, nodename = "localhost", scheduler.latency = 1, fs.latency = 65) { # nocov start
  if (!is.null(clusters))
    assertString(clusters, min.chars = 1L)
  assertFlag(array.jobs)
  assertString(nodename)
  template = findTemplateFile(template)
  template = cfReadBrewTemplate(template, "##")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    jc$clusters = clusters
    if (jc$array.jobs) {
      logs = sprintf("%s_%i", basename(jc$log.file), seq_row(jc$jobs))
      jc$log.file = stri_join(jc$log.file, "_%a")
    }
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("sbatch", shQuote(outfile), nodename = nodename)

    max.jobs.msg = "sbatch: error: Batch job submission failed: Job violates accounting policy (job submit limit, user's size and/or time limits)"
    temp.error = "Socket timed out on send/recv operation"
    output = stri_flatten(stri_trim_both(res$output), "\n")

    if (stri_detect_fixed(max.jobs.msg, output)) {
      makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = max.jobs.msg)
    } else if (stri_detect_fixed(temp.error, output)) {
      # another temp error we want to catch
      makeSubmitJobResult(status = 2L, batch.id = NA_character_, msg = temp.error)
    } else if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("sbatch", res$exit.code, res$output)
    } else {
      id = stri_split_fixed(output[1L], " ")[[1L]][4L]
      if (jc$array.jobs) {
        if (!array.jobs)
          stop("Array jobs not supported by cluster function")
        makeSubmitJobResult(status = 0L, batch.id = sprintf("%s_%i", id, seq_row(jc$jobs)), log.file = logs)
      } else {
        makeSubmitJobResult(status = 0L, batch.id = id)
      }
    }
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    if (array.jobs)
      args = c(args, "-r")
    res = runOSCommand("squeue", args, nodename = nodename)
    if (res$exit.code > 0L)
      OSError("Listing of jobs failed", res)
    if (!is.null(clusters)) tail(res$output, -1L) else res$output
  }

  listJobsQueued = function(reg) {
    args = c("-h", "-o %i", "-u $USER", "-t PD", sprintf("--clusters=%s", clusters))
    listJobs(reg, args)
  }

  listJobsRunning = function(reg) {
    args = c("-h", "-o %i", "-u $USER", "-t R,S,CG", sprintf("--clusters=%s", clusters))
    listJobs(reg, args)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "scancel", c(sprintf("--clusters=%s", clusters), batch.id), nodename = nodename)
  }

  makeClusterFunctions(name = "Slurm", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    listJobsQueued = listJobsQueued, array.var = "SLURM_ARRAY_TASK_ID", store.job.collection = TRUE,
    store.job.files = !isLocalHost(nodename), scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
