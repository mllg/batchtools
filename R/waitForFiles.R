# use list.files() here as this seems to trick the nfs cache
# see https://github.com/mllg/batchtools/issues/85
waitForFiles = function(path, fns, timeout = NA_real_) {
  if (is.na(timeout))
    return(TRUE)

  fns = fns[!file.exists(fns)]
  if (length(fns) == 0L)
    return(TRUE)

  "!DEBUG [waitForFiles]: `length(fns)` files not found via 'file.exists()'"
  fns = chsetdiff(fns, list.files(path))
  if (length(fns) == 0L)
    return(TRUE)

  timeout = timeout + Sys.time()
  repeat {
    Sys.sleep(0.5)
    fns = chsetdiff(fns, list.files(path))
    if (length(fns) == 0L)
      return(TRUE)
    if (Sys.time() > timeout)
      stopf("Timeout while waiting for %i files, e.g. '%s'", length(fns), fns[1L])
  }
}

waitForResults = function(reg, ids) {
  waitForFiles(
    fp(reg$file.dir, "results"),
    sprintf("%i.rds", .findDone(reg, ids)$job.id),
    reg$cluster.functions$fs.latency
  )
}
