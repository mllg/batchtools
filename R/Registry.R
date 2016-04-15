#' @title Registry
#'
#' @description
#' \code{makeRegistry} constructs the inter-communication object for all functions in \code{batchtools}.
#' The registry created last is saved in the package namespace (unless \code{make.default} is set to
#' \code{FALSE}) and can be retrieved via \code{getDefaultRegistry}.
#'
#' \code{saveRegistry} serializes the registry to the file system.
#' \code{syncRegistry} refreshes the registry by parsing updates from remote jobs, merging job
#' status information in the internal data base.
#' Both functions are called internally whenever required.
#' Therefore it should be safe to quit the R session at any time.
#' Work can later be resumed by calling \code{loadRegistry} which de-serializes the registry from
#' the file system.
#'
#' Canceled jobs and repeatedly submitted jobs leave in some cases stray files behind which can be
#' swept using \code{sweepRegistry}.
#' \code{clearRegistry} completely erases all jobs from a registry, including log files and results.
#'
#' @param file.dir [\code{character(1)}]\cr
#'   Path where all files of the registry are saved.
#'   Default is directory \dQuote{registry} in the current working directory.
#'
#'   If you pass \code{NA}, a temporary directory will be used.
#'   This way, you can create disposable registries for \code{\link{btlapply}} or examples.
#'   By default, the temporary directory \code{\link[base]{tempdir}()} will be used.
#'   If you want to use another temp directory, e.g. a directory which is shared between nodes,
#'   you can set it in your configuration via \code{temp.dir}.
#' @param work.dir [\code{character(1)}]\cr
#'   Working directory for R process when experiment is executed.
#'   For \code{makeRegistry}, this defaults to the current working directory.
#'   \code{loadRegistry} uses the stored \code{work.dir}, but you may also explicitly provide one
#'   yourself.
#' @param conf.file [\code{character(1)}]\cr
#'   Path to a configuration file which is sourced directly after the registry is created.
#'   For example, you can set system-specific cluster functions in it.
#'   The script is executed inside the registry environment, thus you can directly set
#'   all slots, e.g. \dQuote{debug = TRUE} would overwrite the debug flag of the registry.
#' @param packages [\code{character}]\cr
#'   Packages that will always be loaded on each node.
#'   Uses \code{\link[base]{require}} internally.
#'   Default is \code{character(0)}.
#' @param namespaces [\code{character}]\cr
#'   Same as \code{packages}, but the packages will not be attached.
#'   Uses \code{\link[base]{requireNamespace}} internally.
#'   Default is \code{character(0)}.
#' @param source [\code{character}]\cr
#'   Files which should be sourced on the slaves prior to executing a job.
#'   Calls \code{\link[base]{sys.source}} using the \code{\link[base]{.GlobalEnv}}.
#' @param load [\code{character}]\cr
#'   Files which should be loaded on the slaves prior to executing a job.
#'   Calls \code{\link[base]{load}} using the \code{\link[base]{.GlobalEnv}}.
#' @param seed [\code{integer(1)}]\cr
#'   Start seed for jobs. Each job uses the (\code{seed} + \code{job.id}) as seed.
#'   Default is a random number in the range [1, \code{.Machine$integer.max/2}].
#' @param make.default [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the created registry is saved inside the package
#'   namespace and acts as default registry. You might want to switch this
#'   off if you work with multiple registries simultaneously.
#'   Default is \code{TRUE}.
#' @return [\code{Registry}]. An environment with the following slots:
#'   \describe{
#'     \item{\code{file.dir} [path]:}{File directory.}
#'     \item{\code{work.dir} [path]:}{Working directory.}
#'     \item{\code{temp.dir} [path]:}{Temporary directory. Only used if \code{file.dir} is \code{NA}.}
#'     \item{\code{packages} [character()]:}{Packages to load on the slaves.}
#'     \item{\code{namespaces} [character()]:}{Namespaces to load on the slaves.}
#'     \item{\code{seed} [integer(1)]:}{Registry seed. Before each job is executed, the seed \code{seed + job.id} is set.}
#'     \item{\code{debug} [logical(1)]:}{Flag to turn additional debug functionality on.}
#'     \item{\code{cluster.functions} [cluster.functions]:}{Usually set in your \code{conf.file}. Set via a call to \code{\link{makeClusterFunctions}}. See example.}
#'     \item{\code{default.resources} [named list()]:}{Usually set in your \code{conf.file}. Named list of default resources.}
#'     \item{\code{max.concurrent.jobs} [integer(1)]:}{Usually set in your \code{conf.file}. Maximum number of concurrent jobs for a single user on the system. \code{\link{submitJobs}} will try to respect this setting.}
#'     \item{\code{defs} [data.table]:}{Table with job definitions (i.e. parameters).}
#'     \item{\code{status} [data.table]:}{Table holding information about the computational status. Also see \code{\link{getJobStatus}}.}
#'     \item{\code{resources} [data.table]:}{Table holding information about the computational resources used for the job. Also see \code{\link{getJobResources}}.}
#'   }
#' @aliases Registry
#' @name Registry
#' @rdname Registry
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' print(reg)
#'
#' #' Set debug mode
#' reg$debug = TRUE
#'
#' # Set cluster functions to interactive mode (default)
#' reg$cluster.functions = makeClusterFunctionsInteractive()
#'
#' # Change default packages
#' reg$packages = c("MASS")
#' saveRegistry(reg = reg)
makeRegistry = function(file.dir = "registry", work.dir = getwd(), conf.file = "~/.batchtools.conf.r", packages = character(0L), namespaces = character(0L),
  source = character(0L), load = character(0L), seed = NULL, make.default = TRUE) {
  assertString(file.dir, na.ok = TRUE)
  if (!is.na(file.dir))
    assertPathForOutput(file.dir, overwrite = FALSE)
  assertString(work.dir)
  assertDirectory(work.dir, access = "r")
  assertString(conf.file)
  assertCharacter(packages, any.missing = FALSE, min.chars = 1L)
  assertCharacter(namespaces, any.missing = FALSE, min.chars = 1L)
  assertCharacter(source, any.missing = FALSE, min.chars = 1L)
  assertCharacter(load, any.missing = FALSE, min.chars = 1L)
  assertFlag(make.default)
  seed = if (is.null(seed)) as.integer(runif(1L, 1, .Machine$integer.max / 2L)) else asCount(seed, positive = TRUE)

  reg = new.env(parent = asNamespace("batchtools"))

  reg$file.dir = file.dir
  reg$work.dir = work.dir
  reg$temp.dir = tempdir()
  reg$packages = packages
  reg$namespaces = namespaces
  reg$source = source
  reg$load = load
  reg$seed = seed
  reg$writeable = TRUE
  reg$debug = FALSE
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$default.resources = list()

  reg$defs = data.table(
    def.id    = integer(0L),
    pars      = list(),
    pars.hash = character(),
    key = "def.id")

  reg$status = data.table(
    job.id      = integer(0L),
    def.id      = integer(0L),
    submitted   = integer(0L),
    started     = integer(0L),
    done        = integer(0L),
    error       = character(0L),
    memory      = double(0L),
    resource.id = integer(0L),
    batch.id    = character(0L),
    job.hash    = character(0L),
    key = "job.id")

  reg$resources = data.table(
    resource.id    = integer(0L),
    resource.hash  = character(0L),
    resources      = list(),
    key = "resource.id")

  if (file.exists(conf.file)) {
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)

    if (!is.null(reg$cluster.functions))
      assertClass(reg$cluster.functions, "ClusterFunctions")
    if (!is.null(reg$default.resources))
      assertList(reg$default.resources, names = "unique")
    if (!is.null(reg$debug))
      assertFlag(reg$debug)
  }

  loadRegistryDependencies(list(work.dir = work.dir, packages = packages, namespaces = namespaces, source = source, load = load), switch.wd = TRUE)

  if (is.na(file.dir)) {
    reg$file.dir = tempfile("registry", tmpdir = npath(reg$temp.dir))
  } else {
    reg$file.dir = npath(file.dir)
  }

  for (d in file.path(reg$file.dir, c("jobs", "results", "updates", "logs")))
    dir.create(d, recursive = TRUE)

  setattr(reg, "class", "Registry")
  saveRegistry(reg)
  if (make.default)
    batchtools$default.registry = reg
  return(reg)
}

#' @export
#' @rdname Registry
getDefaultRegistry = function() {
  if (is.null(batchtools$default.registry))
    stop("No default registry defined")
  batchtools$default.registry
}

#' @export
#' @rdname Registry
setDefaultRegistry = function(reg) {
  assertRegistry(reg)
  batchtools$default.registry = reg
}

#' @export
#' @rdname Registry
clearDefaultRegistry = function() {
  batchtools$default.registry = NULL
  invisible(TRUE)
}

#' @export
print.Registry = function(x, ...) {
  catf("Job Registry")
  catf("  ClusterFunctions: %s", x$cluster.functions$name)
  catf("  Number of Jobs  : %i", nrow(x$status))
  catf("  File dir        : %s", x$file.dir)
  catf("  Work dir        : %s", x$work.dir)
  catf("  Seed            : %i", x$seed)
}

#' @export
#' @param update.paths [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the \code{file.dir} and \code{work.dir} will be updated in the registry. Note that this is
#'   likely to break computation on the system, only do this if no jobs are currently running. Default is \code{FALSE}.
#'   If the provided \code{file.dir} does not match the stored \code{file.dir}, \code{loadRegistry} will return a
#'   registry in an read-only mode.
#' @rdname Registry
loadRegistry = function(file.dir = "registry", work.dir = NULL, conf.file = "~/.batchtools.conf.r",
  make.default = TRUE, update.paths = FALSE) {

  readRegistry = function() {
    fns = file.path(file.dir, c("registry.new.rds", "registry.rds"))
    for (fn in fns) {
      reg = try(readRDS(file.path(file.dir, "registry.rds")), silent = TRUE)
      if (!is.error(reg))
        return(reg)
      warning(sprintf("Registry file '%s' is corrupt", fn))
    }
    stop("Could not load the registry, files seem to be corrupt")
  }

  assertString(file.dir)
  assertFlag(make.default)
  assertFlag(update.paths)

  reg = readRegistry()

  if (update.paths) {
    reg$file.dir = npath(file.dir)
  } else if (npath(file.dir) != npath(reg$file.dir)) {
      warning("The absolute path of the file.dir has changed. Enabling read-only mode for the registry.")
      reg$file.dir = npath(file.dir)
      reg$writeable = FALSE
  }

  if (!is.null(work.dir))
    reg$work.dir = npath(work.dir)
  if (!dir.exists(reg$work.dir))
    warningf("The work.dir '%s' does not exist, jobs might fail to run on this system.", reg$work.dir)

  loadRegistryDependencies(reg, switch.wd = TRUE)
  reg$cluster.functions = makeClusterFunctionsInteractive()
  if (file.exists(conf.file)) {
    parent.env(reg) = .GlobalEnv
    sys.source(conf.file, envir = reg, keep.source = FALSE)
    parent.env(reg) = emptyenv()
  }
  if (make.default)
    batchtools$default.registry = reg
  syncRegistry(reg = reg)
  return(reg)
}

#' @rdname Registry
#' @export
#' @template reg
saveRegistry = function(reg = getDefaultRegistry()) {
  if (reg$writeable) {
    fn = file.path(reg$file.dir, c("registry.new.rds", "registry.rds"))
    writeRDS(reg, file = fn[1L], wait = TRUE)
    file.rename(fn[1L], fn[2L])
  }
  invisible(TRUE)
}

#' @rdname Registry
#' @export
sweepRegistry = function(reg = getDefaultRegistry()) {
  result.files = list.files(file.path(reg$file.dir, "results"))
  i = which(as.integer(stri_replace_last_fixed(result.files, ".rds", "")) %nin% .findDone(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i obsolete result files", length(i))
    file.remove(file.path(reg$file.dir, "results", result.files))
  }

  log.files = list.files(file.path(reg$file.dir, "logs"))
  i = which(stri_replace_last_fixed(log.files, ".log", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete log files", length(i))
    file.remove(file.path(reg$file.dir, "logs", log.files[i]))
  }

  job.files = list.files(file.path(reg$file.dir, "jobs"))
  i = which(stri_replace_last_fixed(job.files, ".rds", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete job files", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.files[i]))
  }

  i = which(reg$resources$resource.id %nin% reg$status$resource.id)
  if (length(i) > 0L) {
    info("Removing %i resource specifications", length(i))
    reg$resources = reg$resources[-i]
  }

  saveRegistry(reg)
}


#' @rdname Registry
#' @export
clearRegistry = function(reg = getDefaultRegistry()) {
  syncRegistry(reg = reg)
  info("Removing %i jobs", nrow(reg$status))
  reg$status = reg$status[FALSE]
  reg$defs = reg$defs[FALSE]
  reg$resources = reg$resources[FALSE]
  user.fun = file.path(reg$file.dir, "user.function.rds")
  if (file.exists(user.fun)) {
    info("Removing user function")
    file.remove(user.fun)
  }
  sweepRegistry(reg = reg)
}

loadRegistryDependencies = function(x, switch.wd = TRUE) {
  ok = vlapply(x$packages, require, character.only = TRUE)
  if (!all(ok))
    stopf("Failed to load packages: %s", stri_join(x$packages[!ok], collapse = ", "))

  ok = vlapply(x$namespaces, requireNamespace)
  if (!all(ok))
    stopf("Failed to load namespaces: %s", stri_join(x$namespaces[!ok], collapse = ", "))

  if (switch.wd) {
    wd = getwd()
    on.exit(setwd(wd))
    setwd(x$work.dir)
  }

  if (length(x$source) > 0L) {
    assertFile(x$source)
    lapply(x$source, sys.source, envir = .GlobalEnv)
  }

  if (length(x$load) > 0L) {
    assertFile(x$load)
    lapply(x$load, load, envir = .GlobalEnv)
  }

  invisible(TRUE)
}

assertRegistry = function(reg, writeable = FALSE, strict = FALSE) {
  assertClass(reg, "Registry", ordered = strict)
  if (writeable & !reg$writeable)
    stop("Registry must be writeable")
  invisible(TRUE)
}

#' @rdname Registry
#' @param save [\code{logical(1)}]\cr
#'   Save the registry to the file system? Default is \code{TRUE}.
#' @export
syncRegistry = function(reg = getDefaultRegistry(), save = TRUE) {
  fns = list.files(file.path(reg$file.dir, "updates"), full.names = TRUE)
  if (length(fns) == 0L)
    return(invisible(TRUE))

  if (reg$writeable) {
    info("Syncing %i files ...", length(fns))
  } else {
    info("Skipping %i updates in read-only mode ...", length(fns))
    return(invisible(TRUE))
  }

  runHook(reg, "pre.sync", fns)

  updates = lapply(fns, function(fn) {
    x = try(readRDS(fn), silent = TRUE)
    if (is.error(x)) NULL else x
  })

  failed = vlapply(updates, is.null)
  updates = rbindlist(updates)

  if (nrow(updates) > 0L) {
    reg$status[updates,
      c("started", "done", "error", "memory") := list(get("i.started"), get("i.done"), get("i.error"), get("i.memory")),
      on = "job.id", nomatch = 0L
    ]
    if (save)
      saveRegistry(reg)
    unlink(fns[!failed])
  }

  runHook(reg, "post.sync", updates)
  invisible(TRUE)
}
