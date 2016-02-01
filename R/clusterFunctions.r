#' @title ClusterFunctions Constructor
#'
#' @description
#' Use this function when you implement a back-end for a batch system. You must define the functions
#' specified in the arguments.
#'
#' @param name [\code{character(1)}]\cr
#'   Name of cluster functions.
#' @param submitJob [\code{function(reg, jc, ...)}]\cr
#'   Function to submit new jobs. Must return a \code{\link{SubmitJobResult}} object.
#'   The arguments are \code{reg} (\code{\link{Registry}}) and \code{jobs} (\code{\link{JobCollection}}).
#'   Set \code{submitJob} to \code{NULL} if killing jobs is not supported.
#' @param killJob [\code{function(reg, batch.id)}]\cr
#'   Function to kill a job on the batch system. Make sure that you definitely kill the job! Return
#'   value is currently ignored. Must have the arguments \code{reg} (\code{\link{Registry}}) and
#'   \code{batch.id} (\code{character(1)}, returned by \code{submitJob}).
#'   Set \code{killJob} to \code{NULL} if killing jobs cannot be supported.
#' @param listJobsQueued [\code{function(reg)}]\cr
#'   List all queued jobs on the batch system for the current user and registry.
#'   Must return an character vector of batch ids, same format as they
#'   are produced by \code{submitJob}. It does not matter if you return a few job ids too many (e.g.
#'   all for the current user instead of all for the current registry), but you have to include all
#'   relevant ones. Must have the argument are \code{reg} (\code{\link{Registry}}).
#'   Set \code{listJobsQueued} to \code{NULL} if listing queued jobs is not supported.
#' @param listJobsRunning [\code{function(reg)}]\cr
#'   List all running jobs on the batch system for the current user and registry. This includes
#'   running, held, idle, etc. jobs. Must return an character vector of batch ids, same format as they
#'   are produced by \code{submitJob}. It does not matter if you return a few job ids too many (e.g.
#'   all for the current user instead of all for the current registry), but you have to include all
#'   relevant ones. Must have the argument are \code{reg} (\code{\link{Registry}}).
#'   Set \code{listJobsRunning} to \code{NULL} if listing jobs is not supported.
#' @param array.envir.var [\code{character(1)}]\cr
#'   Name of the environment variable set by the scheduler to identify IDs of job arrays. Default is
#'   \code{NA} for no array support.
#' @param store.job [\code{logical(1)}]\cr
#'   Store the job on the file system before submitting? Default is \code{TRUE}.
#' @export
#' @aliases ClusterFunctions
#' @family ClusterFunctions
makeClusterFunctions = function(name, submitJob, killJob = NULL, listJobsQueued = NULL, listJobsRunning = NULL, array.envir.var = NA_character_, store.job = TRUE) {
  assertString(name)
  if (!is.null(submitJob))
    assertFunction(submitJob, c("reg", "jc"))
  if (!is.null(killJob))
    assertFunction(killJob, c("reg", "batch.id"))
  if (!is.null(listJobsQueued))
    assertFunction(listJobsQueued, "reg")
  if (!is.null(listJobsRunning))
    assertFunction(listJobsRunning, "reg")
  assertString(array.envir.var, na.ok = TRUE)
  assertFlag(store.job)

  setClasses(list(
      name = name,
      submitJob = submitJob,
      listJobsQueued = listJobsQueued,
      listJobsRunning = listJobsRunning,
      killJob = killJob,
      array.envir.var = array.envir.var,
      store.job = store.job),
    "ClusterFunctions")
}

#' @export
print.ClusterFunctions = function(x, ...) {
  catf("ClusterFunctions for mode: %s", x$name)
  catf("\tSupport for listing queued Jobs: %s", !is.null(x$listJobsQueued))
  catf("\tSupport for listing running Jobs: %s", !is.null(x$listJobsRunning))
  catf("\tSupport for killing Jobs: %s", !is.null(x$killJob))
}

#' @title Create a SubmitJobResult object
#'
#' @description
#' Use this function in your implementation of \code{\link{makeClusterFunctions}} to create a return
#' value for the \code{submitJob} function.
#' @param status [\code{integer(1)}]\cr
#'   Launch status of job. 0 means success, codes between 1 and 100 are temporary errors and any
#'   error greater than 100 is a permanent failure.
#' @param batch.id [\code{character(1)}]\cr
#'   Unique id of this job on batch system. Note that this is not the usual job id used in
#'   BatchJobs! Must be globally unique so that the job can be terminated using just this
#'   information.
#' @param msg [\code{character(1)}]\cr
#'   Optional error message in case \code{status} is not equal to 0. Default is \dQuote{OK},
#'   \dQuote{TEMPERROR}, \dQuote{ERROR}, depending on \code{status}.
#' @return [\code{\link{SubmitJobResult}}]. A list, containing
#'   \code{status}, \code{batch.id} and \code{msg}.
#' @family ClusterFunctionsHelper
#' @aliases SubmitJobResult
#' @export
makeSubmitJobResult = function(status, batch.id, msg = NA_character_) {
  status = asInt(status)
  if (testScalarNA(msg)) {
    msg = if (status == 0L)
      "OK"
    else if (status <= 100L)
      "TEMPERROR"
    else
      "ERROR"
  }
  setClasses(list(status = status, batch.id = batch.id, msg = msg), "SubmitJobResult")
}

#' @export
print.SubmitJobResult = function(x, ...) {
  cat("Job submission result:\n")
  catf("  ID     : %s", x$batch.id)
  catf("  Status : %i", x$status)
  catf("  Msg    : %s", x$msg)
}

#' @title Cluster functions helper: Read in your brew template file
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Simply reads your template and returns it as a character vector. If you do this in the
#' constructor of your cluster functions once, you can avoid this repeated file access later on.
#'
#' @template template
#' @param comment.string [\code{character(1)}]\cr
#'   Ignore lines starting with this string.
#' @return [\code{character}].
#' @family ClusterFunctionsHelper
#' @export
cfReadBrewTemplate = function(template, comment.string = NA_character_) {
  assertFile(template, "r")
  lines = stri_trim_both(readLines(template))
  lines = lines[!stri_isempty(lines)]
  if (!is.na(comment.string))
    lines = lines[!stri_startswith_fixed(lines, comment.string)]
  if (length(lines) == 0L)
    stopf("Error reading template '%s' or empty template", template)
  paste0(lines, collapse = "\n")
}

#' @title Cluster functions helper: Brew your template into a job description file
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Calls brew silently on your template, any error will lead to an exception. If debug mode is
#' enabled, the file is stored at the same place as the corresponding R script in the
#' \dQuote{jobs}-subdir of your files directory, otherwise in the temp dir via
#' \code{\link{tempfile}}.
#'
#' @template reg
#' @param template [\code{character(1)}]\cr
#'   String with contents of the brew templated, as returned by \code{\link{cfReadBrewTemplate}}.
#' @param jc [\code{\link{JobCollection})}]\cr
#'   JobCollection holding all essential information. Will be used as environment to look
#'   up variables.
#' @return [\code{character(1)}]. File path to resulting template file.
#' @family ClusterFunctionsHelper
#' @export
cfBrewTemplate = function(reg, template, jc) {
  assertString(template, "r", na.ok = FALSE)

  outfile = if (reg$debug) file.path(reg$file.dir, "jobs", sprintf("%s.job", jc$job.hash)) else tempfile("job")
  parent.env(jc) = .GlobalEnv
  z = try(brew::brew(text = template, output = outfile, envir = jc), silent = TRUE)
  parent.env(jc) = as.environment("package:batchtools")
  if (is.error(z))
    stopf("Error brewing template: %s", as.character(z))
  return(outfile)
}

#' @title Cluster functions helper: Handle an unknown error during job submission.
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Simply constructs a \code{\link{SubmitJobResult}} object with status code 101, NA as batch id and
#' an informative error message containing the output of the OS command in \code{output}.
#'
#' @param cmd [\code{character(1)}]\cr
#'   OS command used to submit the job, e.g. qsub.
#' @param exit.code [\code{integer(1)}]\cr
#'   Exit code of the OS command, should not be 0.
#' @param output [\code{character}]\cr
#'   Output of the OS command, hopefully an informative error message.
#'   If these are multiple lines in a vector, they are automatically joined.
#' @return [\code{\link{SubmitJobResult}}].
#' @family ClusterFunctionsHelper
#' @export
cfHandleUnknownSubmitError = function(cmd, exit.code, output) {
  assertString(cmd)
  exit.code = asInt(exit.code)
  assertCharacter(output, any.missing = FALSE)
  msg = sprintf("Command '%s' produced exit code %i. Output: '%s'", cmd, exit.code, paste0(output, collapse = "\n"))
  makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = msg)
}

#' @title Cluster functions helper: Kill a batch job via OS command
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Calls the OS command to kill a job via \code{system} like this: \dQuote{cmd batch.job.id}. If the
#' command returns an exit code > 0, the command is repeated after a 1 second sleep
#' \code{max.tries-1} times. If the command failed in all tries, an exception is generated.
#'
#' @param cmd [\code{character(1)}]\cr
#'   OS command, e.g. \dQuote{qdel}.
#' @param batch.id [\code{character(1)}]\cr
#'   Id of the batch job on the batch system.
#' @param max.tries [\code{integer(1)}]\cr
#'   Number of total times to try execute the OS command in cases of failures.
#'   Default is \code{3}.
#' @return Nothing.
#' @family ClusterFunctionsHelper
#' @export
cfKillBatchJob = function(cmd, batch.id, max.tries = 3L) {
  assertString(cmd)
  assertString(batch.id)
  max.tries = asCount(max.tries)

  for (tmp in seq_len(max.tries)) {
    res = runOSCommand(cmd, batch.id, stop.on.exit.code = FALSE)
    if (res$exit.code == 0L)
      return()
    Sys.sleep(1)
  }
  stopf("Really tried to kill job, but could not do it. batch id is %s.\nMessage: %s",
        batch.id, paste0(res$output, collapse = "\n"))
}

getBatchIds = function(reg, status = "all") {
  cf = reg$cluster.functions
  tab = data.table(batch.id = character(0L), status = character(0L))

  if (status %in% c("all", "running") && !is.null(cf$listJobsRunning)) {
    x = cf$listJobsRunning(reg)
    if (length(x) > 0L)
      tab = rbind(tab, data.table(batch.id = unique(x), status = "running"))
  }

  if (status %in% c("all", "queued") && !is.null(cf$listJobsQueued)) {
    x = setdiff(cf$listJobsQueued(reg), tab$batch.id)
    if (length(x) > 0L)
      tab = rbind(tab, data.table(batch.id = unique(x), status = "queued"))
  }

  tab[batch.id %in% reg$status$batch.id]
}

runOSCommand = function(sys.cmd, sys.args = character(0L), nodename = "localhost", stop.on.exit.code = TRUE, debug = FALSE) {
  assertCharacter(sys.cmd, any.missing = FALSE, len = 1L)
  assertCharacter(sys.args, any.missing = FALSE)
  assertString(nodename)
  assertFlag(stop.on.exit.code)
  assertFlag(debug)

  if (nodename != "localhost") {
    sys.args = c(nodename, shQuote(paste0(c(sys.cmd, sys.args), collapse = " ")))
    sys.cmd = "ssh"
  }

  if (debug) {
    info("OS cmd: %s %s", sys.cmd, paste0(sys.args, collapse = " "))
  }

  if (nzchar(Sys.which(sys.cmd)) > 0L) {
    res = suppressWarnings(system2(command = sys.cmd, args = sys.args, stdout = TRUE, stderr = TRUE, wait = TRUE))
    output = as.character(res)
    exit.code = attr(res, "status") %??% 0L
  } else {
    output = "command not found"
    exit.code = 127L
  }

  if (stop.on.exit.code && exit.code > 0L) {
    output = if (length(output) == 0L) "" else paste0(output, collapse = "\n")
    stopf("Command '%s %s' produced exit code: %i; output: %s", sys.cmd, paste0(sys.args, collapse = " "), exit.code, output)
  } else if (debug) {
    catf("OS result (exit code %i):", exit.code)
    print(output)
  }

  return(list(exit.code = exit.code, output = output))
}
