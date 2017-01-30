#' @title Check Consistency and Remove Obsolete Information
#'
#' @description
#' Canceled jobs and jobs submitted multiple times may leave stray files behind.
#' This function checks the registry for consistency and removes obsolete files
#' and data base informations.
#'
#' @template reg
#' @family Registry
#' @export
sweepRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE, writeable = TRUE)
  store = FALSE
  "!DEBUG sweepRegistry: Running sweepRegistry"

  # TODO: use data table keys here
  path = getResultPath(reg)
  result.files = list.files(path, pattern = "\\.rds$")
  has.result = funion(.findDone(reg = reg), .findOnSystem(reg = reg))
  i = which(as.integer(stri_replace_last_fixed(result.files, ".rds", "")) %nin% has.result$job.id)
  if (length(i) > 0L) {
    info("Removing %i obsolete result files ...", length(i))
    file.remove(file.path(path, result.files[i]))
  }

  path = getLogPath(reg)
  log.files = list.files(path, pattern = "\\.log$")
  i = which(stri_replace_last_fixed(log.files, ".log", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete log files ...", length(i))
    file.remove(file.path(path, log.files[i]))
  }

  path = getJobPath(reg)
  job.files = list.files(path, pattern = "\\.rds$")
  i = which(stri_replace_last_fixed(job.files, ".rds", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete job files ...", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.files[i]))
  }

  path = getJobPath(reg)
  job.desc.files = list.files(path, pattern = "\\.job$")
  if (length(job.desc.files) > 0L) {
    info("Removing %i job description files ...", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.desc.files))
  }

  path = getExternalPath(reg)
  external.dirs = list.files(path, pattern = "^[0-9]+$")
  i = which(as.integer(external.dirs) %nin% .findSubmitted(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i external directories of unsubmitted jobs ...", length(i))
    unlink(getExternalDirs(reg$file.dir, external.dirs[i]), recursive = TRUE)
  }

  i = reg$resources[!reg$status, on = "resource.id", which = TRUE]
  if (length(i) > 0L) {
    info("Removing %i resource specifications ...", length(i))
    reg$resources = reg$resources[-i]
    store = TRUE
  }

  i = reg$tags[!reg$status, on = "job.id", which = TRUE]
  if (length(i) > 0L) {
    info("Removing %i tags ...", length(i))
    reg$tags = reg$tags[-i]
    store = TRUE
  }

  if (store) saveRegistry(reg) else FALSE
}
