#' @title Store the Registy to the File System
#'
#' @description
#' Stores the registry on the file system. Usually called internally by functions
#' whenever they touch the registry.
#'
#' @template reg
#' @return [\code{logical(1)}]: \code{TRUE} if the registry was saved,
#'   \code{FALSE} otherwise (if the registry is read-only).
#' @family Registry
#' @export
saveRegistry = function(reg = getDefaultRegistry()) {
  if (reg$writeable) {
    "!DEBUG [saveRegistry]: Saving Registry"

    fn = file.path(reg$file.dir, c("registry.new.rds", "registry.rds"))
    ee = new.env(parent = asNamespace("batchtools"))
    list2env(mget(setdiff(ls(reg), c("cluster.functions", "default.resources", "temp.dir")), reg), ee)
    class(ee) = class(reg)
    writeRDS(ee, file = fn[1L], wait = TRUE)
    file.rename(fn[1L], fn[2L])
  } else {
    "!DEBUG [saveRegistry]: Skipping saveRegistry (read-only)"
  }
  invisible(reg$writeable)
}
