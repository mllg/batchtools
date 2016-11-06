#' @title ClusterFunctions for Docker
#'
#' @description
#' Cluster functions for Docker/Docker Swarm.
#'
#' The \code{submitJob} function executes \code{docker [docker.args] run --detach=true [image.args] [resources] [image] [cmd]}.
#' Arguments \code{docker.args}, \code{image.args} and \code{image} can be set on construction.
#' The \code{resources} part takes the named resources \code{ncpus} and \code{memory} from \code{\link{submitJobs}} and maps them to
#' the arguments \code{--cpu-shares} and \code{--memory} (in Megabytes).
#' To reliably identify jobs in the swarm, jobs are labeled with \dQuote{batchtools=[job.hash]} and named using the current login name and the job hash.
#'
#' \code{listJobsRunning} uses \code{docker [docker.args] ps --format=\{\{.ID\}\}} to filter for running jobs.
#'
#' \code{killJobs} uses \code{docker [docker.args] kill [batch.id]} to filter for running jobs.
#'
#' These cluster functions use a \link{Hook} to remove finished jobs before a new submit and every time the \link{Registry}
#' is synchronized (using \code{\link{syncRegistry}}).
#' This is currently required because docker does not remove exited containers automatically.
#' Use \code{docker ps -a --filter 'label=batchtools' --filter 'status=exited'} to identify and remove terminated containers manually (or via a cron job).
#'
#' @param image [\code{character(1)}]\cr
#'   Name of the docker image to run.
#' @param docker.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker} *before* the command (\dQuote{run}, \dQuote{ps} or \dQuote{kill}) to execute (e.g., the docker host).
#' @param image.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker run} (e.g., to define mounts or environment variables).
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsDocker = function(image, docker.args = character(0L), image.args = character(0L)) { # nocov start
  assertString(image)
  assertCharacter(docker.args, any.missing = FALSE)
  assertCharacter(image.args, any.missing = FALSE)
  user = Sys.info()["user"]

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    assertIntegerish(jc$resources$ncpus, lower = 1L, any.missing = FALSE, .var.name = "resources$ncpus")
    assertIntegerish(jc$resources$memory, lower = 1L, any.missing = FALSE, .var.name = "resources$memory")
    timeout = if (is.null(jc$resources$walltime)) character(0L) else sprintf("timeout %i", asInt(jc$resources$walltime, lower = 0L))

    cmd = c("docker", docker.args, "run", "--detach=true", image.args,
      sprintf("-e DEBUGME=%s", Sys.getenv("DEBUGME")),
      sprintf("-c %i", jc$resources$ncpus),
      sprintf("-m %im", jc$resources$memory),
      sprintf("--label batchtools=%s", jc$job.hash),
      sprintf("--label user=%s", user),
      sprintf("--name=%s_bt_%s", user, jc$job.hash),
      image, timeout, "Rscript", stri_join("-e", shQuote(sprintf("batchtools::doJobCollection('%s', '%s')", jc$uri, jc$log.file)), sep = " "))
    res = runOSCommand(cmd[1L], cmd[-1L])

    if (res$exit.code > 0L) {
      no.res.msg = "no resources available"
      if (res$exit.code == 1L && any(stri_detect_fixed(res$output, no.res.msg)))
        return(makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = no.res.msg))
      return(cfHandleUnknownSubmitError(stri_join(cmd, collapse = " "), res$exit.code, res$output))
    } else {
      if (length(res$output != 1L)) {
        matches = which(stri_detect_regex(res$output, "^[[:alnum:]]{64}$"))
        if (length(matches) != 1L)
          stopf("Command '%s' did not return a long UUID identitfier", stri_join(cmd, collapse = " "))
        res$output = res$output[matches]
      }
      return(makeSubmitJobResult(status = 0L, batch.id = stri_sub(res$output, 1L, 12L)))
    }
  }

  housekeeping = function(reg, ...) {
    args = c(docker.args, "ps", "-a", "--format={{.ID}}", "--filter 'label=batchtools'", "--filter 'status=exited'")
    batch.ids = intersect(runOSCommand("docker", args)$output, reg$status$batch.id)
    if (length(batch.ids) > 0L)
      runOSCommand("docker", c(docker.args, "rm", batch.ids))
    invisible(TRUE)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "docker", c(docker.args, "kill", batch.id))
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    args = c(docker.args, "ps", "--format={{.ID}}", "--filter 'label=batchtools'", sprintf("--filter 'user=%s'", user))
    res = runOSCommand("docker", args)
    if (res$exit.code == 0L)
      return(res$output)
    stop("docker returned non-zero exit code")
  }

  makeClusterFunctions(name = "Docker", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    store.job = TRUE, hooks = list(pre.submit = housekeeping, post.sync = housekeeping))
} # nocov end
