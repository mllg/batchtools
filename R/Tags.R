addJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids, default = copy(noids))
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_]+$", min.len = 1L)

  for (cur in tags) {
    ids[, "tag" := cur, with = FALSE]
    reg$tags = rbind(reg$tags, ids)
  }
  reg$tags = setkeyv(unique(reg$tags, by = NULL), "job.id")

  saveRegistry(reg)
  invisible(TRUE)
}

removeJobTags = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  ids = convertIds(reg, ids)
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_]+$", min.len = 1L)

  if (is.null(ids)) {
    i = reg$tags[tag %in% tags, which = TRUE]
  } else {
    i = reg$tags[job.id %in% ids$job.id & tag %in% tags, which = TRUE]
  }

  if (length(i) > 0L) {
    reg$tags = reg$tags[-i]
    saveRegistry(reg)
  }

  invisible(TRUE)
}

getUsedJobTags = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids)
  tag = NULL
  inner_join(reg$tags, ids)[, unique(tag), on = "job.id", nomatch = 0L]
}

getJobTags = function(ids = NULL, joined = TRUE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = ids(reg$status))
  assertFlag(joined)
  tag = NULL

  if (joined) {
    reg$tags[ids, on = "job.id"][, list(tags = stri_join(tag, collapse = ",")), by = job.id]
  } else {
    reg$tags[ids, on = "job.id"][, list(tags = list(c(tag))), by = job.id]
  }
}

findTagged = function(ids = NULL, tags, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids)
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_]+$", min.len = 1L)
  tag = NULL

  ids(inner_join(reg$tags, convertIds(reg, ids))[tag %in% tags])
}

if (FALSE) {
  reg = makeRegistry(NA)
  batchMap(identity, 1:10)
  addJobTags(1:4, "walltime")
  addJobTags(3:7, "broken")
  getUsedJobTags()
  findTagged(tags = "walltime")
  key2(reg$tags)
  key(reg$tags)

  removeJobTags(9:3, "walltime")
}
