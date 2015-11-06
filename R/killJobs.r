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
#' @return [\code{data.table}] with columns \dQuote{job.id}, the corresponding \dQuote{batch.id} and
#'   the flag \dQuote{killed} to indicate success.
#' @family debug
#' @export
killJobs = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findOnSystem(reg))

  kill = reg$cluster.functions$killJob
  if (is.null(kill))
    stop("ClusterFunction implementation does not support the killing of jobs")

  done = NULL
  ids = reg$status[ids][is.na(done), c("job.id", "started", "batch.id"), with = FALSE]
  ids = ids[findOnSystem(ids = ids, reg = reg)]
  if (nrow(ids) == 0L)
    return(data.table(job.id = integer(0L), batch.id = character(0L), killed = logical(0L)))

  info("Trying to kill %i jobs ...", nrow(ids))

  # kill queued jobs first, otherwise they might get started while killing running jobs
  setorderv(ids, "started", na.last = FALSE)
  ids[, "killed" := FALSE]

  batch.ids = unique(ids$batch.id)
  info("Killing %i real batch jobs ...", length(batch.ids))

  for (i in 1:3) {
    ids[!ids$killed, "killed" := is.error(try(kill(reg, .BY), silent = TRUE)), by = "batch.id"]
    if (all(ids$killed))
      break
    Sys.sleep(2)
  }

  if (!all(ids$killed))
    warning("Could not kill %i jobs", sum(!ids$killed))

  # reset killed jobs
  syncRegistry(reg = reg)
  reg$status[ids[ids$killed], c("submitted", "started", "done", "error", "memory", "resource.id", "batch.id", "job.hash") := list(NA_integer_, NA_integer_, NA_integer_, NA_character_, NA_real_, NA_integer_, NA_character_, NA_character_)]

  saveRegistry(reg)
  ids[, c("job.id", "batch.id", "killed"), with = FALSE]
}
