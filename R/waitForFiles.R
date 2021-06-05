# use list.files() here as this seems to trick the nfs cache
# see https://github.com/mllg/batchtools/issues/85
waitForFiles = function(path, fns, timeout = 0) {
  if (timeout == 0)
    return(TRUE)

  fns = fns[!fs::file_exists(fns)]
  if (length(fns) == 0L)
    return(TRUE)

  "!DEBUG [waitForFiles]: `length(fns)` files not found via 'file.exists()'"
  fns = chsetdiff(fns, list.files(path, all.files = TRUE))
  if (length(fns) == 0L)
    return(TRUE)

  timeout = timeout + Sys.time()
  repeat {
    Sys.sleep(0.5)
    fns = chsetdiff(fns, list.files(path, all.files = TRUE))
    if (length(fns) == 0L)
      return(TRUE)
    if (Sys.time() > timeout)
      stopf("Timeout while waiting for %i files, e.g. '%s'", length(fns), fns[1L])
  }
}

waitForFile = function(fn, timeout = 0, must.work = TRUE) {
  if (timeout == 0 || fs::file_exists(fn))
    return(TRUE)

  "!DEBUG [waitForFile]: `fn` not found via 'file.exists()'"
  timeout = timeout + Sys.time()
  path = fs::path_dir(fn)
  fn = fs::path_file(fn)
  repeat {
    Sys.sleep(0.5)
    if (fn %chin% list.files(path, all.files = TRUE))
      return(TRUE)
    if (Sys.time() > timeout) {
      if (must.work)
        stopf("Timeout while waiting for file '%s'", file.path(path, fn))
      return(FALSE)
    }
  }
}

