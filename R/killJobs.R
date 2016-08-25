#' @title Kill Jobs
#'
#' @description
#' Kill jobs which have already been submitted to the batch system.
#' If a job is killed its internal state is reset as if it had not been submitted at all.
#'
#' In case of an error when killing, the function tries - after a short sleep - to kill the remaining
#' batch jobs again. If this fails again for some jobs, the function gives up. Only jobs that could be
#' killed are reset in the DB.
#'
#' @templateVar ids.default findOnSystem
#' @template ids
#' @template reg
#' @return [\code{\link{data.table}}] with columns \dQuote{job.id}, the corresponding \dQuote{batch.id} and
#'   the flag \dQuote{killed} to indicate success.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @family debug
#' @export
killJobs = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE)

  kill = reg$cluster.functions$killJob
  if (is.null(kill))
    stop("ClusterFunction implementation does not support the killing of jobs")

  ids = convertIds(reg, ids, default = .findSubmitted(reg = reg))
  tab = reg$status[.findOnSystem(ids = ids, reg = reg), c("job.id", "started", "batch.id"), with = FALSE]

  if (nrow(tab) == 0L)
    return(data.table(job.id = integer(0L), batch.id = character(0L), killed = logical(0L)))

  info("Trying to kill %i jobs ...", nrow(tab))

  # kill queued jobs first, otherwise they might get started while killing running jobs
  setorderv(tab, "started", na.last = FALSE)
  tab[, "killed" := FALSE]

  batch.ids = unique(tab$batch.id)
  info("Killing %i real batch jobs ...", length(batch.ids))

  for (i in seq_len(3L)) {
    tab[!tab$killed, "killed" := !is.error(try(kill(reg, .BY$batch.id), silent = TRUE)), by = "batch.id"]
    if (all(tab$killed))
      break
    Sys.sleep(2)
  }

  if (!all(tab$killed))
    warningf("Could not kill %i jobs", sum(!tab$killed))

  # reset killed jobs
  syncRegistry(reg = reg)
  reg$status[tab[tab$killed], c("submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash") := list(NA_integer_, NA_integer_, NA_integer_, NA_character_, NA_real_, NA_integer_, NA_character_, NA_character_)]

  saveRegistry(reg)
  tab[, c("job.id", "batch.id", "killed"), with = FALSE]
}
