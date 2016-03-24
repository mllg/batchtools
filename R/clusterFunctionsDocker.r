#' @title ClusterFunctions for Docker
#'
#' @description
#' Experimental cluster functions for Docker/Docker Swarm.
#'
#' The \code{submitJobs} function executes \code{docker [docker.args] run --detach=true [image.args] [resources] [image] [cmd]}.
#' Arguments \code{docker.args}, \code{image.args} and \code{image} can be set via arguments on construction.
#' The \code{resources} part takes the named resources \code{ncpus} and \code{memory} from \code{\link{submitJobs}} and maps them to
#' the arguments \code{--cpu-shares} and \code{--memory} (in Megabytes).
#'
#' \code{listJobsRunning} uses \code{docker [docker.args] ps --format=\{\{.ID\}\}} to filter for running jobs.
#'
#' \code{killJobs} uses \code{docker [docker.args] kill [batch.id]} to filter for running jobs.
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
makeClusterFunctionsDocker = function(image, docker.args = character(0L), image.args = character(0L)) {
  assertString(image)
  assertCharacter(docker.args, any.missing = FALSE)
  assertCharacter(image.args, any.missing = FALSE)

  submitJob = function(reg, jc) {
    assertIntegerish(jc$resources$ncpus, lower = 1L, any.missing = FALSE, .var.name = "resources$ncpus")
    assertIntegerish(jc$resources$memory, lower = 1L, any.missing = FALSE, .var.name = "resources$memory")

    cmd = c("docker", docker.args, "run", "--detach=true", image.args,
      sprintf("-c %i", jc$resources$ncpus),
      sprintf("-m %im", jc$resources$memory),
      sprintf("--label batchtools=%s", jc$job.hash),
      sprintf("--name=%s_batchtools_%s", Sys.info()["user"], jc$job.hash),
      image, "Rscript", stri_join("-e", shQuote(sprintf("batchtools::doJobCollection('%s', '%s')", jc$uri, jc$log.file)), sep = " "))
    res = runOSCommand(cmd[1L], cmd[-1L], stop.on.exit.code = FALSE, debug = reg$debug)

    if (res$exit.code > 0L) {
      no.res.msg = "no resources available to schedule container"
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

  listJobsRunning = function(reg) {
    res = runOSCommand("docker", c(docker.args, "ps", "--format={{.ID}}", "--filter 'label=batchtools'"), debug = reg$debug)
    if (res$exit.code == 0L)
      return(res$output)
    stop("docker returned non-zero exit code")
  }

  killJob = function(reg, batch.id) {
    cfKillBatchJob("docker", c(docker.args, "kill", batch.id))
  }

  makeClusterFunctions(name = "Docker", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning, store.job = TRUE,
    hooks = list(pre.submit = housekeeping, post.sync = housekeeping))
}
