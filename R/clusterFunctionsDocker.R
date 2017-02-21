#' @title ClusterFunctions for Docker
#'
#' @description
#' Cluster functions for Docker/Docker Swarm (\url{https://docs.docker.com/swarm/}).
#'
#' The \code{submitJob} function executes
#' \code{docker [docker.args] run --detach=true [image.args] [resources] [image] [cmd]}.
#' Arguments \code{docker.args}, \code{image.args} and \code{image} can be set on construction.
#' The \code{resources} part takes the named resources \code{ncpus} and \code{memory}
#' from \code{\link{submitJobs}} and maps them to the arguments \code{--cpu-shares} and \code{--memory}
#' (in Megabytes). The resource \code{threads} is mapped to the environment variables \dQuote{OMP_NUM_THREADS}
#' and \dQuote{OPENBLAS_NUM_THREADS}.
#' To reliably identify jobs in the swarm, jobs are labeled with \dQuote{batchtools=[job.hash]} and named
#' using the current login name (label \dQuote{user}) and the job hash (label \dQuote{batchtools}).
#'
#' \code{listJobsRunning} uses \code{docker [docker.args] ps --format=\{\{.ID\}\}} to filter for running jobs.
#'
#' \code{killJobs} uses \code{docker [docker.args] kill [batch.id]} to filter for running jobs.
#'
#' These cluster functions use a \link{Hook} to remove finished jobs before a new submit and every time the \link{Registry}
#' is synchronized (using \code{\link{syncRegistry}}).
#' This is currently required because docker does not remove terminated containers automatically.
#' Use \code{docker ps -a --filter 'label=batchtools' --filter 'status=exited'} to identify and remove terminated
#' containers manually (or usa a cron job).
#'
#' @param image [\code{character(1)}]\cr
#'   Name of the docker image to run.
#' @param docker.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker} *before* the command (\dQuote{run}, \dQuote{ps} or \dQuote{kill}) to execute (e.g., the docker host).
#' @param image.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker run} (e.g., to define mounts or environment variables).
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsDocker = function(image, docker.args = character(0L), image.args = character(0L), scheduler.latency = 1, fs.latency = 65) { # nocov start
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
      sprintf("-e DEBUGME='%s'", Sys.getenv("DEBUGME")),
      sprintf("-e OMP_NUM_THREADS=%i", jc$resources$threads %??% 1L),
      sprintf("-e OPENBLAS_NUM_THREADS=%i", jc$resources$threads %??% 1L),
      sprintf("-c %i", jc$resources$ncpus),
      sprintf("-m %im", jc$resources$memory),
      sprintf("--memory-swap %im", jc$resources$memory),
      sprintf("--label batchtools=%s", jc$job.hash),
      sprintf("--label user=%s", user),
      sprintf("--name=%s_bt_%s", user, jc$job.hash),
      image, timeout, "Rscript", stri_join("-e", shQuote(sprintf("batchtools::doJobCollection('%s', '%s')", jc$uri, jc$log.file)), sep = " "))
    res = runOSCommand(cmd[1L], cmd[-1L])

    if (res$exit.code > 0L) {
      no.res.msg = "no resources available"
      if (res$exit.code == 1L && any(stri_detect_fixed(res$output, no.res.msg)))
        return(makeSubmitJobResult(status = 1L, batch.id = NA_character_, msg = no.res.msg))
      return(cfHandleUnknownSubmitError(stri_flatten(cmd, " "), res$exit.code, res$output))
    } else {
      if (length(res$output != 1L)) {
        matches = which(stri_detect_regex(res$output, "^[[:alnum:]]{64}$"))
        if (length(matches) != 1L)
          stopf("Command '%s' did not return a long UUID identitfier", stri_flatten(cmd, " "))
        res$output = res$output[matches]
      }
      return(makeSubmitJobResult(status = 0L, batch.id = stri_sub(res$output, 1L, 12L)))
    }
  }

  listJobs = function(reg, filter = character(0L)) {
    # use a workaround for DockerSwarm: docker ps does not list all jobs correctly, only
    # docker inspect reports the status correctly
    args = c(docker.args, "ps", "--format={{.ID}}", "--filter 'label=batchtools'", filter)
    ids = runOSCommand("docker", args)
    if (ids$exit.code != 0L)
      stop("docker returned non-zero exit code")
    if (length(ids$output) == 0L)
      return(character(0L))

    args = c(docker.args, "inspect", "--format '{{json .State.Status}}'", ids$output)
    status = runOSCommand("docker", args)
    ids$output[status$output == "\"running\""]
  }

  housekeeping = function(reg, ...) {
    batch.ids = chintersect(listJobs(reg, "--filter 'status=exited'"), reg$status$batch.id)
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
    listJobs(reg, sprintf("--filter 'user=%s'", user))
  }

  makeClusterFunctions(name = "Docker", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    store.job = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency,
    hooks = list(pre.submit.job = housekeeping, post.sync = housekeeping))
} # nocov end
