#' @title Synchronize the Registry
#'
#' @description
#' Parses update files written by the slaves to the file system and updates the
#' internal data base.
#'
#' @template reg
#' @return [\code{logical(1)}]: \code{TRUE} if the state has changed, \code{FALSE} otherwise.
#' @family Registry
#' @export
syncRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg)
  altered = sync(reg)
  if (altered)
    saveRegistry(reg)
  altered
}


sync = function(reg) {
  "!DEBUG [syncRegistry]: Triggered syncRegistry"
  fns = list.files(dir(reg, "updates"), full.names = TRUE)
  if (length(fns) == 0L)
    return(invisible(FALSE))

  runHook(reg, "pre.sync", fns = fns)

  updates = lapply(fns, function(fn) {
    x = try(readRDS(fn), silent = TRUE)
    if (is.error(x)) {
      if (reg$writeable && difftime(Sys.time(), fs::file_info(fn)$modification_time, units = "mins") > 60) {
        info("Removing unreadable update file '%s'", fn)
        file_remove(fn)
      } else {
        info("Skipping unreadable update file '%s'", fn)
      }
      return(NULL)
    }
    return(x)
  })

  failed = vlapply(updates, is.null)
  updates = rbindlist(updates, fill = TRUE) # -> fill = TRUE for #135

  if (nrow(updates) > 0L) {
    expr = quote(`:=`(started = i.started, done = i.done, error = i.error, mem.used = i.mem.used))
    reg$status[updates, eval(expr), on = "job.id"]
    if (reg$writeable)
      file_remove(fns[!failed])
  }

  runHook(reg, "post.sync", updates = updates)
  invisible(nrow(updates) > 0L)
}
