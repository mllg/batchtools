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
  "!DEBUG [syncRegistry]: Triggered syncRegistry"
  fns = list.files(getUpdatePath(reg), full.names = TRUE)
  if (length(fns) == 0L)
    return(invisible(TRUE))

  if (reg$writeable) {
    info("Syncing %i files ...", length(fns))
  } else {
    info("Skipping %i updates in read-only mode ...", length(fns))
    return(invisible(FALSE))
  }

  runHook(reg, "pre.sync", fns = fns)

  updates = lapply(fns, function(fn) {
    x = try(readRDS(fn), silent = TRUE)
    if (is.error(x)) NULL else x
  })

  failed = vlapply(updates, is.null)
  updates = rbindlist(updates)

  if (nrow(updates) > 0L) {
    expr = quote(`:=`(started = i.started, done = i.done, error = i.error, memory = i.memory))
    reg$status[updates, eval(expr), on = "job.id"]
    saveRegistry(reg)
    unlink(fns[!failed])
  }

  runHook(reg, "post.sync", updates = updates)
  invisible(TRUE)
}
