#' @title Reset the Computational State of Jobs
#'
#' @description
#' Resets the computational state of jobs in the \code{\link{Registry}}.
#' This function automatically checks if any of the jobs to reset is either pending or running.
#' However, if the implemented heuristic fails, this can lead to inconsistencies in the data base.
#' Use with care while jobs are running.
#'
#' @templateVar ids.default none
#' @template ids
#' @template reg
#' @return [\code{\link{data.table}}] of job ids which have been reset.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @family debug
#' @export
resetJobs = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE, running.ok = FALSE)
  ids = convertIds(reg, ids, default = noIds())

  info("Resetting %i jobs in DB.", nrow(ids))
  cols = c("submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash")
  reg$status[ids, (cols) := list(NA_integer_, NA_integer_, NA_integer_, NA_character_, NA_real_, NA_integer_, NA_character_, NA_character_), on = "job.id"]

  sweepRegistry(reg)
  invisible(ids)
}
