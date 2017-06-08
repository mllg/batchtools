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
  assertRegistry(reg, writeable = TRUE, running.ok = FALSE)
  "!DEBUG [sweepRegistry]: Running sweepRegistry"

  submitted = reg$status[.findSubmitted(reg = reg), c("job.id", "job.hash", "log.file")]
  path = reg$uri$path[["results"]]
  obsolete = chsetdiff(
    list.files(path, full.names = TRUE),
    reg$uri$results(submitted)
  )
  info("Removing %i obsolete result files ...", length(obsolete))
  file.remove(obsolete)

  path = reg$uri$path[["logs"]]
  obsolete = chsetdiff(
    list.files(path, full.names = TRUE),
    reg$uri$logs(submitted)
  )
  info("Removing %i obsolete log files ...", length(obsolete))
  file.remove(obsolete)

  path = reg$uri$path[["jobs"]]
  obsolete = list.files(path, pattern = "\\.rds", full.names = TRUE)
  info("Removing %i obsolete job collection files ...", length(obsolete))
  file.remove(obsolete)

  path = reg$uri$path[["jobs"]]
  obsolete = list.files(path, pattern = "\\.job$", full.names = TRUE)
  info("Removing %i job description files ...", length(obsolete))
  file.remove(obsolete)

  path = reg$uri$path[["external"]]
  obsolete = chsetdiff(
    list.files(path, pattern = "^[0-9]+$", full.names = TRUE),
    reg$uri$external(submitted)
  )
  info("Removing %i external directories of unsubmitted jobs ...", length(obsolete))
  unlink(obsolete, recursive = TRUE)

  obsolete = reg$resources[!reg$status, on = "resource.id", which = TRUE]
  info("Removing %i resource specifications ...", length(obsolete))
  if (length(obsolete) > 0L)
    reg$resources = reg$resources[-obsolete]

  obsolete = reg$tags[!reg$status, on = "job.id", which = TRUE]
  info("Removing %i tags ...", length(obsolete))
  if (length(obsolete) > 0L)
    reg$tags = reg$tags[-obsolete]

  saveRegistry(reg)
}
