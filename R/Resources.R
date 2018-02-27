if (FALSE) {
  reg = makeRegistry(NA)

  fun = function(...) list(...)
  ids = batchMap(fun, i = 1:3, reg = reg)
  ids$walltime = as.integer(c(180, 120, 180))
  ids$chunk = 1:3
  submitAndWait(reg, ids = ids, resources = list(walltime = 120))

  unwrap(getJobResources())

  setJobResources(1, list(walltime = 122))
  setJobResources(2:3, list(walltime = 123))

  unwrap(getJobResources())


  unwrap(reg$resources)
  reg$status
}


#' @export
getJobResources = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = allIds(reg))

  tab = merge(
    filter(reg$status, ids, c("job.id", "resource.id")),
    reg$resources,
    by = "resource.id", sort = FALSE, all.x = TRUE
  )[, c("job.id", "resources")]
  setkeyv(tab, "job.id")[]
}

#' @export
setJobResources = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = allIds(reg))

  if (is.null(resources)) {
    reg$status[ids, resource.id = NA_integer_, on = "job.id"]
    sweepResources(reg = reg)
  } else {
    assertList(resources, names = "unique", min.len = 1L)
    chunk.res = chintersect(batchtools$resources$per.chunk, names(resources))
    if (length(chunk.res))
      stopf("Setting per-chunk resources like '%s' is not supported", chunk.res[1L])
    rid = addResources(reg, resources)
    reg$status[.(ids), "resource.id" := rid]
  }
  invisible(TRUE)
}

addResources = function(reg, resources) {
  hash = digest(resources)
  res.id = reg$resources[hash, "resource.id", on = "resource.hash"][[1L]]
  if (is.na(res.id)) {
    res.id = if (nrow(reg$resources)) max(reg$resources$resource.id) + 1L else 1L
    reg$resources = rbindlist(list(
      reg$resources,
      data.table(resource.id = res.id, resource.hash = hash, resources = list(resources))
    ))
    setkeyv(reg$resources, "resource.id")
  }

  return(res.id)
}
