#' @title Load a Registry from the File System
#'
#' @description
#' Loads a registry from its \code{file.dir}.
#'
#' Multiple R sessions accessing the same registry simultaneously can lead to database inconsistencies.
#' This is especially dangerous if the same \code{file.dir} is accessed from multiple machines, e.g. via a mount.
#'
#' If you just need to check on the status or peek into some preliminary results while another process is still submitting or waiting
#' for pending results, you can load the registry in a read-only mode.
#' All operations that need to change the registry will raise an exception in this mode.
#' Files communicated back by the computational nodes are parsed to update the registry in memory while the registry on the file system remains unchanged.
#'
#' A heuristic tries to detect if the registry has been altered in the background by an other process and in this case automatically restricts the current registry to read-only mode.
#' However, you should rely on this heuristic to work flawlessly.
#' Thus, set to \code{writeable} to \code{TRUE} if and only if you are absolutely sure that other state-changing processes are terminated.
#'
#' If you need write access, load the registry with \code{writeable} set to \code{TRUE}.
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
  if (writeable)
    info("Reading registry in read-write mode")
  else
    info(stri_paste(
        "Reading registry in read-only mode.",
        "You can inspect results and errors, but cannot add, remove, submit or alter jobs in any way.",
        "If you need write-access, re-load the registry with `loadRegistry([...], writeable = TRUE)`."
    ))
  file.dir = path_real(file.dir)
  reg = readRegistry(file.dir)

  # re-allocate stuff which has not been serialized
  reg$file.dir = file.dir
  reg$writeable = writeable
  reg$mtime = file_mtime(fs::path(reg$file.dir, "registry.rds"))
  alloc.col(reg$status, ncol(reg$status))
  alloc.col(reg$defs, ncol(reg$defs))
  alloc.col(reg$resources, ncol(reg$resources))
  alloc.col(reg$tags, ncol(reg$tags))
  if (!is.null(work.dir))
    reg$work.dir = path_real(work.dir)
  updated = updateRegistry(reg = reg)

  # try to load dependencies relative to work.dir
  if (fs::dir_exists(reg$work.dir)) {
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
  fn.old = fs::path(file.dir, "registry.rds")
  fn.new = fs::path(file.dir, "registry.new.rds")

  if (fs::file_exists(fn.new)) {
    reg = try(readRDS(fn.new), silent = TRUE)
    if (!is.error(reg)) {
      fs::file_move(fn.new, fn.old)
      return(reg)
    } else {
      warning("Latest version of registry seems to be corrupted, trying backup ...")
    }
  }

  if (fs::file_exists(fn.old)) {
    reg = try(readRDS(fn.old), silent = TRUE)
    if (!is.error(reg))
      return(reg)
    stop("Could not load the registry, files seem to be corrupt")
  }

  stopf("No registry found in '%s'", file.dir)
}
