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
  "!DEBUG Running sweepRegistry"

  result.files = list.files(file.path(reg$file.dir, "results"), pattern = "\\.rds$")
  i = which(as.integer(stri_replace_last_fixed(result.files, ".rds", "")) %nin% .findSubmitted(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i obsolete result files ...", length(i))
    file.remove(file.path(reg$file.dir, "results", result.files[i]))
  }

  log.files = list.files(file.path(reg$file.dir, "logs"), pattern = "\\.log$")
  i = which(stri_replace_last_fixed(log.files, ".log", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete log files ...", length(i))
    file.remove(file.path(reg$file.dir, "logs", log.files[i]))
  }

  job.files = list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.rds$")
  i = which(stri_replace_last_fixed(job.files, ".rds", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete job files ...", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.files[i]))
  }

  job.desc.files = list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.job$")
  if (length(job.desc.files) > 0L) {
    info("Removing %i job description files ...", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.desc.files))
  }

  external.dirs = list.files(file.path(reg$file.dir, "external"), pattern = "^[0-9]+$")
  i = which(as.integer(external.dirs) %nin% .findSubmitted(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i external directories of unsubmitted jobs ...", length(i))
    unlink(file.path(reg$file.dir, "external", external.dirs[i]), recursive = TRUE)
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
