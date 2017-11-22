#' @title ClusterFunctions for DockerQueue
#'
#' Customized cluster functions for the isolated application running on the SFB876 cluster.
#'
#' @param image [\code{character(1)}]\cr
#'   Name of the docker image to run.
#' @param docker.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker} *before* the command (\dQuote{run}, \dQuote{ps} or \dQuote{kill}) to execute (e.g., the docker host).
#' @param image.args [\code{character}]\cr
#'   Additional arguments passed to \dQuote{docker run} (e.g., to define mounts or environment variables).
#' @param docker.scheduler.url [\code{character}]\cr
#'   URL of the docker scheduler API.
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsDockerQueue = function(image, docker.args = character(0L), image.args = character(0L), scheduler.latency = 1, fs.latency = 65, docker.scheduler.url = "https://s876cnsm:2350/v1.30") { # nocov start
  assertString(image)
  assertCharacter(docker.args, any.missing = FALSE)
  assertCharacter(image.args, any.missing = FALSE)
  docker.scheduler.url = stri_replace_all_regex("test", "\\/$", replacement = "")
  assertCharacter(docker.scheduler.url, any.missing = FALSE)
  user = Sys.info()["user"]

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    assertIntegerish(jc$resources$ncpus, lower = 1L, any.missing = FALSE, .var.name = "resources$ncpus")
    assertIntegerish(jc$resources$memory, lower = 1L, any.missing = FALSE, .var.name = "resources$memory")
    timeout = if (is.null(jc$resources$walltime)) character(0L) else sprintf("timeout %i", asInt(jc$resources$walltime, lower = 0L))

    batch.id = sprintf("%s-bt_%s", user, jc$job.hash)
    cmd = c("docker", docker.args, "create", "--label queue", "--label rm", image.args,
      sprintf("-e DEBUGME='%s'", Sys.getenv("DEBUGME")),
      sprintf("-e OMP_NUM_THREADS=%i", jc$resources$threads %??% 1L),
      sprintf("-e OPENBLAS_NUM_THREADS=%i", jc$resources$threads %??% 1L),
      sprintf("-c %i", jc$resources$ncpus),
      sprintf("-m %im", jc$resources$memory),
      sprintf("--memory-swap %im", jc$resources$memory),
      sprintf("--label batchtools=%s", jc$job.hash),
      sprintf("--label user=%s", user),
      sprintf("--name=%s", batch.id),
      image, timeout, "Rscript", stri_join("-e", shQuote(sprintf("batchtools::doJobCollection('%s', '%s')", jc$uri, jc$log.file)), sep = " "))
    res = runOSCommand(cmd[1L], cmd[-1L])

    if (res$exit.code > 0L) {
      return(cfHandleUnknownSubmitError(stri_flatten(cmd, " "), res$exit.code, res$output))
    } else {
      return(makeSubmitJobResult(status = 0L, batch.id = batch.id))
    }
  }

  listJobs = function(reg) {
    if (!requireNamespace("jsonlite", quietly = TRUE))
      stop("Package 'jsonlite' is required")
    tab = jsonlite::fromJSON(sprintf("%s/jobs/%s/json", docker.scheduler.url, user))
    if (length(tab) == 0L)
      return(data.table(id = integer(0L), batch.id = character(0L), toSchedule = logical(0)))
    tab = as.data.table(tab[, c("id", "containerName", "toSchedule")])[get("containerName") %chin% reg$status$batch.id]
    setnames(tab, "containerName", "batch.id")
    tab[]
  }

  killJob = function(reg, batch.id) {
    if (!requireNamespace("RCurl", quietly = TRUE))
      stop("Package 'RCurl' is required")
    id = listJobs(reg)[batch.id == batch.id]$id
    res = RCurl::httpDELETE(sprintf("%s/jobs/%i/delete", docker.scheduler.url, id))
    stri_startswith_fixed(res, "Successfully deleted")
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg)[get("toSchedule") == FALSE]$batch.id
  }

  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg)[get("toSchedule") == TRUE]$batch.id
  }


  makeClusterFunctions(name = "DockerQueue", submitJob = submitJob, killJob = killJob, listJobsRunning = listJobsRunning,
    listJobsQueued = listJobsQueued, store.job.collection = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
