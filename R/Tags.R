#' @title Add or Remove Job Tags
#' @name Tags
#' @rdname Tags
#'
#' @description
#' Add and remove arbitrary tags to jobs.
#'
#' @templateVar ids.default all
#' @template ids
#' @param tags [\code{character}]\cr
#'   Tags to add or remove as strings. May use letters, numbers, underscore and dots (pattern \dQuote{^[[:alnum:]_.]+}).
#' @return [\code{\link[data.table]{data.table}}] with job ids affected (invisible).
#' @template reg
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(sqrt, x = -3:3, reg = tmp)
#'
#' # Add new tag to all ids
#' addJobTags(ids, "needs.computation", reg = tmp)
#' getJobTags(reg = tmp)
#'
#' # Add more tags
#' addJobTags(findJobs(x < 0, reg = tmp), "x.neg", reg = tmp)
#' addJobTags(findJobs(x > 0, reg = tmp), "x.pos", reg = tmp)
#' getJobTags(reg = tmp)
#'
#' # Submit first 5 jobs and remove tag if successful
#' ids = submitJobs(1:5, reg = tmp)
#' if (waitForJobs(reg = tmp))
#'   removeJobTags(ids, "needs.computation", reg = tmp)
#' getJobTags(reg = tmp)
#'
#' # Grep for warning message and add a tag
#' addJobTags(grepLogs(pattern = "NaNs produced", reg = tmp), "div.zero", reg = tmp)
#' getJobTags(reg = tmp)
#'
#' # All tags where tag x.neg is set:
#' ids = findTagged("x.neg", reg = tmp)
#' getUsedJobTags(ids, reg = tmp)
addJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids, default = allIds(reg))
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_.]+$", min.len = 1L)

  for (cur in tags) {
    ids[, ("tag") := cur]
    reg$tags = rbind(reg$tags, ids)
  }
  reg$tags = setkeyv(unique(reg$tags, by = NULL), "job.id")

  saveRegistry(reg)
  invisible(ids[, "job.id", with = FALSE])
}

#' @export
#' @rdname Tags
removeJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids)
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_.]+$", min.len = 1L)
  job.id = tag = NULL

  if (is.null(ids)) {
    i = reg$tags[tag %in% tags, which = TRUE]
  } else {
    i = reg$tags[job.id %in% ids$job.id & tag %in% tags, which = TRUE]
  }
  if (length(i) > 0L) {
    ids = unique(reg$tags[i, "job.id", with = FALSE], by = "job.id")
    reg$tags = reg$tags[-i]
    saveRegistry(reg)
  } else {
    ids = noIds()
  }

  invisible(ids)
}

#' @export
#' @rdname Tags
getUsedJobTags = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids)

  tag = NULL
  filter(reg$tags, ids)[, unique(tag), on = "job.id", nomatch = 0L]
}
