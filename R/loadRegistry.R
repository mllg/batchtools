#' @title Load a Registry from the File System
#'
#' @description
#' Loads a registry from its \code{file.dir}.
#'
#' Multiple R sessions accessing the same registry simultaneously can lead to database inconsistencies.
#' Here, it does not matter if the sessions run on the same system or different systems via a file system mount.
#'
#' If you just need to check on the status or peek into some preliminary results, you can load the registry in a
#' read-only mode by setting \code{writeable} to \code{FALSE}.
#' All operations that need to change the registry will raise an exception in this mode.
#' Files communicated back by the computational nodes are parsed to update the registry in memory, but remain on the file system
#' in order to be read and cleaned up by an R session with read-write access.
#'
#' A heuristic tries to detect if the registry has been altered in the background.
#' However, you should not completely rely on it.
#' Thus, set to \code{writeable} to \code{TRUE} if and only if you are absolutely sure other R processes are terminated.
#'
#' @param writeable [\code{logical(1)}]\cr
#'   Loads the registry in read-write mode. Default is \code{FALSE}.
#' @inheritParams makeRegistry
#' @family Registry
#' @return [\code{\link{Registry}}].
#' @export
loadRegistry = function(file.dir, work.dir = NULL, conf.file = findConfFile(), make.default = TRUE, writeable = FALSE) {
  assertString(file.dir)
  assertDirectory(file.dir)
  assertString(work.dir, null.ok = TRUE)
  assertCharacter(conf.file, any.missing = FALSE, max.len = 1L)
  assertFlag(make.default)
  assertFlag(writeable)

  # read registry
  info("Reading registry in read-%s mode", ifelse(writeable, "write", "only"))
  file.dir = npath(file.dir)
  reg = readRegistry(file.dir)

  # re-allocate stuff which has not been serialized
  reg$file.dir = file.dir
  reg$writeable = writeable
  reg$mtime = file.mtime(fp(reg$file.dir, "registry.rds"))
  alloc.col(reg$status, ncol(reg$status))
  alloc.col(reg$defs, ncol(reg$defs))
  alloc.col(reg$resources, ncol(reg$resources))
  alloc.col(reg$tags, ncol(reg$tags))
  if (!is.null(work.dir)) reg$work.dir = npath(work.dir)
  updated = updateRegistry(reg = reg)

  # try to load dependencies relative to work.dir
  if (dir.exists(reg$work.dir)) {
    with_dir(reg$work.dir, loadRegistryDependencies(reg))
  } else {
    warningf("The work.dir '%s' does not exist, jobs might fail to run on this system.", reg$work.dir)
    loadRegistryDependencies(reg)
  }

  # source system config
  setSystemConf(reg, conf.file)
  if (make.default)
    batchtools$default.registry = reg

  if (sync(reg = reg) || updated)
    saveRegistry(reg)
  return(reg)
}

readRegistry = function(file.dir) {
  fn.old = fp(file.dir, "registry.rds")
  fn.new = fp(file.dir, "registry.new.rds")

  if (file.exists(fn.new)) {
    reg = try(readRDS(fn.new), silent = TRUE)
    if (!is.error(reg)) {
      file.rename(fn.new, fn.old)
      return(reg)
    } else {
      warning("Latest version of registry seems to be corrupted, trying backup ...")
    }
  }

  if (file.exists(fn.old)) {
    reg = try(readRDS(fn.old), silent = TRUE)
    if (!is.error(reg))
      return(reg)
    stop("Could not load the registry, files seem to be corrupt")
  }

  stopf("No registry found in '%s'", file.dir)
}
