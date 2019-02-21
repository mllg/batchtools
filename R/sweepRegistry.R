#' @title Check Consistency and Remove Obsolete Information
#'
#' @description
#' Canceled jobs and jobs submitted multiple times may leave stray files behind.
#' This function checks the registry for consistency and removes obsolete files
#' and redundant data base entries.
#'
#' @template reg
#' @family Registry
#' @export
sweepRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE, running.ok = FALSE)
  "!DEBUG [sweepRegistry]: Running sweepRegistry"
  submitted = reg$status[.findSubmitted(reg = reg), c("job.id", "job.hash")]

  obsolete = chsetdiff(
    fs::dir_ls(dir(reg, "results"), all = TRUE, recursive = TRUE, type = "file"),
    getResultFiles(reg, submitted)
  )
  if (length(obsolete)) {
    info("Removing %i obsolete result files ...", length(obsolete))
    fs::file_delete(obsolete)
  }

  obsolete = chsetdiff(
    fs::dir_ls(dir(reg, "logs"), all = TRUE, recursive = TRUE, type = "file"),
    getLogFiles(reg, submitted)
  )
  if (length(obsolete)) {
    info("Removing %i obsolete log files ...", length(obsolete))
    fs::file_delete(obsolete)
  }

  obsolete = fs::dir_ls(dir(reg, "jobs"), all = TRUE, glob = "*.rds", type = "file")
  if (length(obsolete)) {
    info("Removing %i obsolete job collection files ...", length(obsolete))
    fs::file_delete(obsolete)
  }

  obsolete = fs::dir_ls(dir(reg, "jobs"), all = TRUE, glob = "*.job", type = "file")
  if (length(obsolete)) {
    info("Removing %i job description files ...", length(obsolete))
    fs::file_delete(obsolete)
  }

  obsolete = chsetdiff(
    fs::dir_ls(dir(reg, "external"), type = "directory"),
    getExternalDirs(reg, submitted)
  )
  if (length(obsolete)) {
    info("Removing %i external directories of unsubmitted jobs ...", length(obsolete))
    fs::dir_delete(obsolete)
  }

  obsolete = reg$resources[!reg$status, on = "resource.id", which = TRUE]
  if (length(obsolete)) {
    info("Removing %i resource specifications ...", length(obsolete))
    reg$resources = reg$resources[-obsolete]
  }

  obsolete = reg$tags[!reg$status, on = "job.id", which = TRUE]
  if (length(obsolete)) {
    info("Removing %i tags ...", length(obsolete))
    reg$tags = reg$tags[-obsolete]
  }

  saveRegistry(reg)
}
