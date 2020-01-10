#' @title Store the Registy to the File System
#'
#' @description
#' Stores the registry on the file system in its \dQuote{file.dir} (specified
#' for construction in \code{\link{makeRegistry}}, can be accessed via
#' \code{reg$file.dir}).
#' This function is usually called internally whenever needed.
#'
#' @template reg
#' @return [\code{logical(1)}]: \code{TRUE} if the registry was saved,
#'   \code{FALSE} otherwise (if the registry is read-only).
#' @family Registry
#' @export
saveRegistry = function(reg = getDefaultRegistry()) {
  if (!reg$writeable) {
    "!DEBUG [saveRegistry]: Skipping saveRegistry (read-only)"
    return(FALSE)
  }

  "!DEBUG [saveRegistry]: Saving Registry"
  reg$hash = rnd_hash()
  fn = fs::path(reg$file.dir, c("registry.new.rds", "registry.rds"))
  ee = new.env(parent = asNamespace("batchtools"))
  exclude = c("cluster.functions", "default.resources", "temp.dir", "mtime", "writeable")
  list2env(mget(chsetdiff(ls(reg), exclude), reg), ee)
  class(ee) = class(reg)
  writeRDS(ee, file = fn[1L], compress = reg$compress)
  fs::file_move(fn[1L], fn[2L])
  reg$mtime = file_mtime(fn[2L])
  return(TRUE)
}
