#' @title Load a Registry from the File System
#' @description
#' Loads a registry from its \code{file.dir}.
#'
#' @param update.paths [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the \code{file.dir} and \code{work.dir} will be updated in the registry. Note that this is
#'   likely to break computation on the system! Only do this if no jobs are currently running. Default is \code{FALSE}.
#'   If the provided \code{file.dir} does not match the stored \code{file.dir}, \code{loadRegistry} will return a
#'   registry in read-only mode.
#' @inheritParams makeRegistry
#' @family Registry
#' @return [\code{\link{Registry}}].
#' @export
loadRegistry = function(file.dir = getwd(), work.dir = NULL, conf.file = findConfFile(), make.default = TRUE, update.paths = FALSE) {
  assertString(file.dir)
  assertCharacter(conf.file, any.missing = FALSE, max.len = 1L)
  assertFlag(make.default)
  assertFlag(update.paths)

  readRegistry = function() {
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

  reg = readRegistry()
  alloc.col(reg$status, ncol(reg$status))
  alloc.col(reg$defs, ncol(reg$defs))
  alloc.col(reg$resources, ncol(reg$resources))
  alloc.col(reg$tags, ncol(reg$tags))

  file.dir = npath(file.dir)
  if (!update.paths) {
    before = npath(reg$file.dir, must.work = FALSE)
    if (before != file.dir) {
      warningf("The absolute path of the file.dir has changed (before '%s', now '%s'). Enabling read-only mode for the registry.", before, file.dir)
      reg$writeable = FALSE
    }
  }
  reg$file.dir = file.dir
  if (reg$writeable)
    updateRegistry(reg)

  if (!is.null(work.dir)) {
    assertString(work.dir)
    reg$work.dir = npath(work.dir)
  }

  if (dir.exists(reg$work.dir)) {
    with_dir(reg$work.dir, loadRegistryDependencies(reg))
  } else {
    warningf("The work.dir '%s' does not exist, jobs might fail to run on this system.", reg$work.dir)
    loadRegistryDependencies(reg)
  }

  reg$cluster.functions = makeClusterFunctionsInteractive()
  setSystemConf(reg, conf.file)
  if (make.default)
    batchtools$default.registry = reg
  syncRegistry(reg = reg)
  return(reg)
}

