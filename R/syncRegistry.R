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
  assertRegistry(reg, writeable = TRUE)
  sync(reg)
  saveRegistry(reg)
}


sync = function(reg) {
  "!DEBUG [syncRegistry]: Triggered syncRegistry"
  fns = list.files(dir(reg, "updates"), full.names = TRUE)
  if (length(fns) == 0L)
    return(invisible(FALSE))

  runHook(reg, "pre.sync", fns = fns)

  updates = lapply(fns, function(fn) {
    x = try(readRDS(fn), silent = TRUE)
    if (is.error(x)) NULL else x
  })

  failed = vlapply(updates, is.null)
  updates = rbindlist(updates, fill = TRUE) # -> fill = TRUE for #135

  if (nrow(updates) > 0L) {
    expr = quote(`:=`(started = i.started, done = i.done, error = i.error, memory = i.memory))
    reg$status[updates, eval(expr), on = "job.id"]
    if (reg$writeable)
      file.remove.safely(fns[!failed])
  }

  runHook(reg, "post.sync", updates = updates)
  invisible(TRUE)
}
