#' @title Add or Remove Job Tags
#' @name Tags
#' @rdname Tags
#'
#' @description
#'
#' @templateVar ids.default all
#' @template ids
#' @param tags [\code{character}]\cr
#'   Tags to add or remove as string. May use letters, numbers, underscore and dots (pattern \dQuote{^[[:alnum:]_.]+}).
#' @return [\code{\link[data.table]{data.table}}] with job ids affected (invisible).
#' @template reg
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' ids = batchMap(sqrt, x = -3:3, reg = reg)
#'
#' # Add new tag to all ids
#' addJobTags(ids, "needs.computation", reg = reg)
#' getJobTags(reg = reg)
#'
#' # Add more tags
#' addJobTags(findJobs(x < 0, reg = reg), "x.neg", reg = reg)
#' addJobTags(findJobs(x > 0, reg = reg), "x.pos", reg = reg)
#' getJobTags(reg = reg)
#'
#' # Submit first 5 jobs and remove tag if successful
#' ids = submitJobs(1:5, reg = reg)
#' if (waitForJobs(reg = reg))
#'   removeJobTags(ids, "needs.computation", reg = reg)
#' getJobTags(reg = reg)
#'
#' # Grep for warning message and add a tag
#' addJobTags(grepLogs(pattern = "NaNs produced", reg = reg), "div.zero", reg = reg)
#' getJobTags(reg = reg)
#'
#' # All tags where tag x.neg is set:
#' ids = findTagged("x.neg", reg = reg)
#' getUsedJobTags(ids, reg = reg)
addJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids, default = ids(reg$status))
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_.]+$", min.len = 1L)

  for (cur in tags) {
    ids[, "tag" := cur, with = FALSE]
    reg$tags = rbind(reg$tags, ids)
  }
  reg$tags = setkeyv(unique(reg$tags, by = NULL), "job.id")

  saveRegistry(reg)
  invisible(ids(ids))
}

#' @export
#' @rdname Tags
removeJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids)
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_.]+$", min.len = 1L)

  if (is.null(ids)) {
    i = reg$tags[tag %in% tags, which = TRUE]
  } else {
    i = reg$tags[job.id %in% ids$job.id & tag %in% tags, which = TRUE]
  }
  ids = unique(ids(reg$tags[i]), by = "job.id")
  if (length(i) > 0L) {
    reg$tags = reg$tags[-i]
    saveRegistry(reg)
  }

  invisible(ids)
}

#' @export
#' @rdname Tags
getUsedJobTags = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids)
  tag = NULL
  inner_join(reg$tags, ids)[, unique(tag), on = "job.id", nomatch = 0L]
}
