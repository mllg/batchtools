#' @title ClusterFunctions Constructor
#'
#' @description
#' This is the constructor used to create \emph{custom} cluster functions.
#' Note that some standard implementations for TORQUE, Slurm, LSF, SGE, etc. ship
#' with the package.
#'
#' @param name [\code{character(1)}]\cr
#'   Name of cluster functions.
#' @param submitJob [\code{function(reg, jc, ...)}]\cr
#'   Function to submit new jobs. Must return a \code{\link{SubmitJobResult}} object.
#'   The arguments are \code{reg} (\code{\link{Registry}}) and \code{jobs} (\code{\link{JobCollection}}).
#' @param killJob [\code{function(reg, batch.id)}]\cr
#'   Function to kill a job on the batch system. Make sure that you definitely kill the job! Return
#'   value is currently ignored. Must have the arguments \code{reg} (\code{\link{Registry}}) and
#'   \code{batch.id} (\code{character(1)} as returned by \code{submitJob}).
#'   Note that there is a helper function \code{\link{cfKillJob}} to repeatedly try to kill jobs.
#'   Set \code{killJob} to \code{NULL} if killing jobs cannot be supported.
#' @param listJobsQueued [\code{function(reg)}]\cr
#'   List all queued jobs on the batch system for the current user.
#'   Must return an character vector of batch ids, same format as they
#'   are returned by \code{submitJob}.
#'   Set \code{listJobsQueued} to \code{NULL} if listing of queued jobs is not supported.
#' @param listJobsRunning [\code{function(reg)}]\cr
#'   List all running jobs on the batch system for the current user.
#'   Must return an character vector of batch ids, same format as they
#'   are returned by \code{submitJob}. It does not matter if you return a few job ids too many (e.g.
#'   all for the current user instead of all for the current registry), but you have to include all
#'   relevant ones. Must have the argument are \code{reg} (\code{\link{Registry}}).
#'   Set \code{listJobsRunning} to \code{NULL} if listing of running jobs is not supported.
#' @param array.var [\code{character(1)}]\cr
#'   Name of the environment variable set by the scheduler to identify IDs of job arrays.
#'   Default is \code{NA} for no array support.
#' @param store.job.collection [\code{logical(1)}]\cr
#'   Flag to indicate that the cluster function implementation of \code{submitJob} can not directly handle \code{\link{JobCollection}} objects.
#'   If set to \code{FALSE}, the \code{\link{JobCollection}} is serialized to the file system before submitting the job.
#' @param store.job.files [\code{logical(1)}]\cr
#'   Flag to indicate that job files need to be stored in the file directory.
#'   If set to \code{FALSE} (default), the job file is created in a temporary directory, otherwise (or if the debug mode is enabled) in
#'   the subdirectory \code{jobs} of the \code{file.dir}.
#' @param scheduler.latency [\code{numeric(1)}]\cr
#'   Time to sleep after important interactions with the scheduler to ensure a sane state.
#'   Currently only triggered after calling \code{\link{submitJobs}}.
#' @param fs.latency [\code{numeric(1)}]\cr
#'   Expected maximum latency of the file system, in seconds.
#'   Set to a positive number for network file systems like NFS which enables more robust (but also more expensive) mechanisms to
#'   access files and directories.
#'   Usually safe to set to \code{NA} to disable the heuristic, e.g. if you are working on a local file system.
#' @param hooks [\code{list}]\cr
#'   Named list of functions which will we called on certain events like \dQuote{pre.submit} or \dQuote{post.sync}.
#'   See \link{Hooks}.
#' @export
#' @aliases ClusterFunctions
#' @family ClusterFunctions
#' @family ClusterFunctionsHelper
makeClusterFunctions = function(name, submitJob, killJob = NULL, listJobsQueued = NULL, listJobsRunning = NULL,
  array.var = NA_character_, store.job.collection = FALSE, store.job.files = FALSE, scheduler.latency = 0,
  fs.latency = NA_real_, hooks = list()) {
  assertList(hooks, types = "function", names = "unique")
  assertSubset(names(hooks), unlist(batchtools$hooks, use.names = FALSE))

  setClasses(list(
      name = assertString(name, min.chars = 1L),
      submitJob = assertFunction(submitJob, c("reg", "jc"), null.ok = TRUE),
      killJob = assertFunction(killJob, c("reg", "batch.id"), null.ok = TRUE),
      listJobsQueued = assertFunction(listJobsQueued, "reg", null.ok = TRUE),
      listJobsRunning = assertFunction(listJobsRunning, "reg", null.ok = TRUE),
      array.var = assertString(array.var, na.ok = TRUE),
      store.job.collection = assertFlag(store.job.collection),
      store.job.files = assertFlag(store.job.files),
      scheduler.latency = assertNumber(scheduler.latency, lower = 0),
      fs.latency = assertNumber(fs.latency, lower = 0, na.ok = TRUE),
      hooks = hooks),
    "ClusterFunctions")
}

#' @export
print.ClusterFunctions = function(x, ...) {
  catf("ClusterFunctions for mode: %s", x$name)
  catf("  List queued Jobs : %s", !is.null(x$listJobsQueued))
  catf("  List running Jobs: %s", !is.null(x$listJobsRunning))
  catf("  Kill Jobs        : %s", !is.null(x$killJob))
  catf("  Hooks            : %s", if (length(x$hooks)) stri_flatten(names(x$hooks), ",") else "-")
}

#' @title Create a SubmitJobResult
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Use this function in your implementation of \code{\link{makeClusterFunctions}} to create a return
#' value for the \code{submitJob} function.
#'
#' @param status [\code{integer(1)}]\cr
#'   Launch status of job. 0 means success, codes between 1 and 100 are temporary errors and any
#'   error greater than 100 is a permanent failure.
#' @param batch.id [\code{character()}]\cr
#'   Unique id of this job on batch system, as given by the batch system.
#'   Must be globally unique so that the job can be terminated using just this information.
#'   For array jobs, this may be a vector of length equal to the number of jobs in the array.
#' @param log.file [\code{character()}]\cr
#'   Log file. If \code{NA}, defaults to \code{[job.hash].log}.
#'   Some cluster functions set this for array jobs.
#' @param msg [\code{character(1)}]\cr
#'   Optional error message in case \code{status} is not equal to 0. Default is \dQuote{OK},
#'   \dQuote{TEMPERROR}, \dQuote{ERROR}, depending on \code{status}.
#' @return [\code{\link{SubmitJobResult}}]. A list, containing
#'   \code{status}, \code{batch.id} and \code{msg}.
#' @family ClusterFunctionsHelper
#' @aliases SubmitJobResult
#' @export
makeSubmitJobResult = function(status, batch.id, log.file = NA_character_, msg = NA_character_) {
  status = asInt(status)
  if (is.na(msg)) {
    msg = if (status == 0L)
      "OK"
    else if (status <= 100L)
      "TEMPERROR"
    else
      "ERROR"
  }
  "!DEBUG [makeSubmitJobResult]: Result for batch.id '`paste0(batch.id, sep = ',')`': `status` (`msg`)"

  setClasses(list(status = status, batch.id = batch.id, log.file = log.file, msg = msg), "SubmitJobResult")
}

#' @export
print.SubmitJobResult = function(x, ...) {
  cat("Job submission result\n")
  catf("  ID    : %s", stri_flatten(x$batch.id, ","))
  catf("  Status: %i", x$status)
  catf("  Msg   : %s", x$msg)
}

#' @title Cluster Functions Helper to Parse a Brew Template
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' This function is only intended for use in your own cluster functions implementation.
#' Simply reads your template file and returns it as a character vector.
#'
#' @param template [\code{character(1)}]\cr
#'   Path to template file or single string (containing newlines) which is then passed
#'   to \code{\link[brew]{brew}}.
#' @param comment.string [\code{character(1)}]\cr
#'   Ignore lines starting with this string.
#' @return [\code{character}].
#' @family ClusterFunctionsHelper
#' @export
cfReadBrewTemplate = function(template, comment.string = NA_character_) {
  if (stri_detect_fixed(template, "\n")) {
    "!DEBUG [cfReadBrewTemplate]: Parsing template from string"
    lines = stri_trim_both(stri_split_lines(template)[[1L]])
  } else {
    "!DEBUG [cfReadBrewTemplate]: Parsing template file '`template`'"
    lines = stri_trim_both(readLines(template))
  }

  lines = lines[!stri_isempty(lines)]
  if (!is.na(comment.string))
    lines = lines[!stri_startswith_fixed(lines, comment.string)]
  if (length(lines) == 0L)
    stopf("Error reading template '%s' or empty template", template)
  return(stri_flatten(lines, "\n"))
}

#' @title Cluster Functions Helper to Write Job Description Files
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Calls brew silently on your template, any error will lead to an exception. If debug mode is
#' enabled (via environment variable \dQuote{DEBUGME} set to \dQuote{batchtools}),
#' the file is stored at the same place as the corresponding
#' job file in the \dQuote{jobs}-subdir of your files directory, otherwise as \code{\link{tempfile}} on the local system.
#'
#' @template reg
#' @param text [\code{character(1)}]\cr
#'   String ready to be brewed. See \code{\link{cfReadBrewTemplate}} to read a template from the file system.
#' @param jc [\code{\link{JobCollection})}]\cr
#'   Will be used as environment to brew the template file in. See \code{\link{JobCollection}} for a list of all
#'   available variables.
#' @return [\code{character(1)}]. File path to brewed template file.
#' @family ClusterFunctionsHelper
#' @export
cfBrewTemplate = function(reg, text, jc) {
  assertString(text)

  path = if (batchtools$debug || reg$cluster.functions$store.job.files) fs::path(reg$file.dir, "jobs") else fs::path_temp()
  fn = sprintf("%s.job", jc$job.hash)
  outfile = fs::path(path, fn)

  parent.env(jc) = asNamespace("batchtools")
  on.exit(parent.env(jc) <- emptyenv())
  "!DEBUG [cfBrewTemplate]: Brewing template to file '`outfile`'"

  z = try(brew(text = text, output = outfile, envir = jc), silent = TRUE)
  if (is.error(z))
    stopf("Error brewing template: %s", as.character(z))
  waitForFiles(path, fn, reg$cluster.functions$scheduler.latency)
  return(outfile)
}

#' @title Cluster Functions Helper to Handle Unknown Errors
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
  assertString(cmd, min.chars = 1L)
  exit.code = asInt(exit.code)
  assertCharacter(output, any.missing = FALSE)
  msg = sprintf("Command '%s' produced exit code %i. Output: '%s'", cmd, exit.code, stri_flatten(output, "\n"))
  makeSubmitJobResult(status = 101L, batch.id = NA_character_, msg = msg)
}

#' @title Cluster Functions Helper to Kill Batch Jobs
#'
#' @description
#' This function is only intended for use in your own cluster functions implementation.
#'
#' Calls the OS command to kill a job via \code{\link[base]{system}} like this: \dQuote{cmd batch.job.id}. If the
#' command returns an exit code > 0, the command is repeated after a 1 second sleep
#' \code{max.tries-1} times. If the command failed in all tries, an error is generated.
#'
#' @template reg
#' @param cmd [\code{character(1)}]\cr
#'   OS command, e.g. \dQuote{qdel}.
#' @param args [\code{character}]\cr
#'   Arguments to \code{cmd}, including the batch id.
#' @param max.tries [\code{integer(1)}]\cr
#'   Number of total times to try execute the OS command in cases of failures.
#'   Default is \code{3}.
#' @inheritParams runOSCommand
#' @return \code{TRUE} on success. An exception is raised otherwise.
#' @family ClusterFunctionsHelper
#' @export
cfKillJob = function(reg, cmd, args = character(0L), max.tries = 3L, nodename = "localhost") {
  assertString(cmd, min.chars = 1L)
  assertCharacter(args, any.missing = FALSE)
  assertString(nodename)
  max.tries = asCount(max.tries)

  for (i in seq_len(max.tries)) {
    res = runOSCommand(cmd, args, nodename = nodename)
    if (res$exit.code == 0L)
      return(TRUE)
    Sys.sleep(1)
  }

  stopf("Really tried to kill job, but failed %i times with '%s'.\nMessage: %s",
    max.tries, stri_flatten(c(cmd, args), " "), stri_flatten(res$output, "\n"))
}

getBatchIds = function(reg, status = "all") {
  cf = reg$cluster.functions
  tab = data.table(batch.id = character(0L), status = character(0L))
  batch.id = NULL

  if (status %chin% c("all", "running") && !is.null(cf$listJobsRunning)) {
    "!DEBUG [getBatchIds]: Getting running Jobs"
    x = unique(cf$listJobsRunning(reg))
    if (length(x) > 0L)
      tab = rbind(tab, data.table(batch.id = x, status = "running"))
  }

  if (status %chin% c("all", "queued") && !is.null(cf$listJobsQueued)) {
    "!DEBUG [getBatchIds]: Getting queued Jobs"
    x = chsetdiff(cf$listJobsQueued(reg), tab$batch.id)
    if (length(x) > 0L)
      tab = rbind(tab, data.table(batch.id = unique(x), status = "queued"))
  }

  tab[batch.id %in% reg$status$batch.id]
}

findTemplateFile = function(name) {
  assertString(name, min.chars = 1L)

  if (stri_detect_fixed(name, "\n"))
    return(name)

  if (stri_endswith_fixed(name, ".tmpl")) {
    return(assertFileExists(name, access = "r"))
  }

  x = Sys.getenv("R_BATCHTOOLS_SEARCH_PATH")
  if (nzchar(x)) {
    x = fs::path(x, sprintf("batchtools.%s.tmpl", name))
    if (testFileExists(x, access = "r"))
      return(fs::path_real(x))
  }

  x = sprintf("batchtools.%s.tmpl", name)
  if (testFileExists(x, access = "r"))
    return(fs::path_real(x))

  x = fs::path(user_config_dir("batchtools", expand = FALSE), sprintf("%s.tmpl", name))
  if (testFileExists(x, access = "r"))
    return(x)

  x = fs::path("~", sprintf(".batchtools.%s.tmpl", name))
  if (testFileExists(x, access = "r"))
    return(fs::path_real(x))

  x = fs::path(site_config_dir("batchtools"), sprintf("%s.tmpl", name))
  if (testFileExists(x, access = "r"))
    return(x)

  x = system.file("templates", sprintf("%s.tmpl", name), package = "batchtools")
  if (testFileExists(x, access = "r"))
    return(x)

  stopf("Argument 'template' (=\"%s\") must point to a readable template file or contain the template itself as string (containing at least one newline)", name)
}
