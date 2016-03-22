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

    cmd = c("docker", docker.args, "run", "--detach=true", image.args, sprintf("-m %iM", jc$resources$memory), sprintf("--name=%s", jc$job.hash), image, stri_join("Rscript -e", sprintf(shQuote("batchtools::doJobCollection('%s')"), jc$uri), sep = " "))
    res = runOSCommand(cmd[1L], cmd[-1L], stop.on.exit.code = FALSE, debug = reg$debug)

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError(stri_join(cmd, collapse = " "), res$exit.code, res$output)
    } else {
      makeSubmitJobResult(status = 0L, batch.id = stri_sub(res$output, 1L, 12L))
    }
  }

  listJobsRunning = function(reg) {
    res = runOSCommand("docker", c(docker.args, "ps", "--format={{.ID}}"), debug = reg$debug)
    if (res$exit.code == 0L)
      return(res$output)
    stop("docker returned non-zero exit code")
  }

  killJob = function(reg, batch.id) {
    cfKillBatchJob("docker", c(docker.args, "kill", batch.id))
  }

  readLog = function(reg, batch.id) {
    res = runOSCommand("docker", c(docker.args, "logs", batch.id))
    if (res$exit.code == 0L)
      return(res$output)
    return(NULL)
  }

  makeClusterFunctions(name = "Docker", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning, readLog = readLog, store.job = TRUE)
}
