#' @export
#' @rdname Registry
getDefaultRegistry = function() {
  if (is.null(batchtools$default.registry))
    stop("No default registry defined")
  batchtools$default.registry
}

#' @export
#' @rdname Registry
clearDefaultRegistry = function() {
  batchtools$default.registry = NULL
  invisible(TRUE)
}

#' @title Construct a Registry Object
#'
#' @description
#' \code{makeRegistry} constructs the inter-communication object for all functions in \code{batchtools}.
#' The registry created last is saved in the package namespace (unless \code{make.default} is set to
#' \code{FALSE}) and can be retrieved via \code{getDefaultRegistry}.
#'
#' \code{makeTempRegistry} creates a temporary registry in the subdirectory of a temporary directory.
#' Such disposable registries are mainly used in sequential apply functions like \code{\link{btlapply}}.
#' The base dir can be set via the option \dQuote{batchtools.temp.dir} and defaults to \code{\link[base]{tempdir}}.
#'
#' \code{saveRegistry} serializes the registry to the file system and be be loaded with \code{loadRegistry} by specifying the \code{file.dir}.
#' \code{syncRegisty} refreshes the registry by parsing update files from jobs.
#'
#' @param file.dir [\code{character(1)}]\cr
#'   Path where all files of the registry are saved.
#'   Default is directory \dQuote{registry} in the current working directory.
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
#'   The default location of the configuration can be set via the option \dQuote{batchtools.conf.file}.
#' @param packages [\code{character}]\cr
#'   Packages that will always be loaded on each node.
#'   Uses \code{\link[base]{require}} internally.
#'   Default is \code{character(0)}.
#' @param namespaces [\code{character}]\cr
#'   Same as \code{packages}, but the packages will not be attached.
#'   Uses \code{\link[base]{requireNamespace}} internally.
#'   Default is \code{character(0)}.
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
#'     \item{file.dir}{File directory.}
#'     \item{work.dir}{Working directory.}
#'     \item{packages}{Packages to load on the slaves.}
#'     \item{namespaces}{Namespaces to load on the slaves.}
#'     \item{seed}{Registry seed. Before each job is executed, the seed \code{seed + job.id} is set.}
#'     \item{debug}{Flag to turn additional debug functionality on.}
#'     \item{cluster.functions}{Usually set in your \code{conf.file}. Set via a call to \code{\link{makeClusterFunctions}}. See example.}
#'     \item{default.resources}{Usually set in your \code{conf.file}. Named list of default resources.}
#'     \item{max.concurrent.jobs}{Usually set in your \code{conf.file}. Maximum number of concurrent jobs for a single user on the system. \code{\link{submitJobs}} will try to respect this setting.}
#'     \item{defs}{Table with job definitions (i.e. parameters).}
#'     \item{status}{Table holding information about the computational status. Also see \code{\link{getJobStatus}}.}
#'     \item{resources}{Table holding information about the computational resources used for the job. Also see \code{\link{getJobResources}}.}
#'   }
#' @aliases Registry
#' @name Registry
#' @rdname Registry
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
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
makeRegistry = function(file.dir = "registry", work.dir = getwd(), conf.file = getOption("batchtools.conf.file", "~/.batchtools.conf.r"), packages = character(0L), namespaces = character(0L), seed = NULL, make.default = TRUE) {
  assertString(file.dir)
  assertPathForOutput(file.dir, overwrite = FALSE)
  assertString(work.dir)
  assertDirectory(work.dir, access = "r")
  assertString(conf.file)
  assertCharacter(packages, any.missing = FALSE)
  assertCharacter(namespaces, any.missing = FALSE)
  assertFlag(make.default)
  seed = if (is.null(seed)) as.integer(runif(1L, 1, .Machine$integer.max / 2L)) else asCount(seed, positive = TRUE)

  loadRegistryPackages(packages, namespaces)

  dir.create(file.dir, recursive = TRUE)
  dir.create(file.path(file.dir, "jobs"))
  dir.create(file.path(file.dir, "results"))
  dir.create(file.path(file.dir, "updates"))
  dir.create(file.path(file.dir, "logs"))

  reg = new.env()

  reg$file.dir = npath(file.dir)
  reg$work.dir = work.dir
  reg$packages = packages
  reg$namespaces = namespaces
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
    resources.hash = character(0L),
    resources      = list(),
    key = "resource.id")

  if (file.exists(conf.file)) {
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)
  }
  parent.env(reg) = emptyenv()

  setattr(reg, "class", "Registry")
  saveRegistry(reg)
  if (make.default)
    batchtools$default.registry = reg
  return(reg)
}

#' @export
#' @param temp.dir [\code{character(1)}]\cr
#'   Path to temporary directory.
#'   Defaults to the value of the option \dQuote{batchtools.temp.dir} or (if unset) \code{\link[base]{tempdir}}.
#' @param ... [\code{ANY}]\cr
#'   Additional parameters passed to \code{makeRegistry}.
#' @rdname Registry
makeTempRegistry = function(make.default = FALSE, temp.dir = getOption("batchtools.temp.dir", tempdir()), ...) {
  makeRegistry(file.dir = file.path(temp.dir, basename(tempfile("registry"))), make.default = make.default, ...)
}

#' @export
print.Registry = function(x, ...) {
  catf("Job Registry")
  catf("  Number of jobs: %i", nrow(x$status))
  catf("  File dir: %s", x$file.dir)
  catf("  Work dir: %s", x$work.dir)
  catf("  Seed: %i", x$seed)
}

#' @export
#' @param update.paths [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the \code{file.dir} and \code{work.dir} will be updated in the registry. Note that this is
#'   likely to break computation on the system, only do this if no jobs are currently running. Default is \code{FALSE}.
#'   If the provided \code{file.dir} does not match the stored \code{file.dir}, \code{loadRegistry} will return a
#'   registry in an read-only mode.
#' @rdname Registry
loadRegistry = function(file.dir = "registry", work.dir = NULL, conf.file = getOption("batchtools.conf.file", "~/.batchtools.conf.r"),
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

  loadRegistryPackages(reg$packages, reg$namespaces)
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
}

loadRegistryPackages = function(packages, namespaces) {
  ok = vlapply(packages, require, character.only = TRUE)
  if (!all(ok))
    stopf("Failed to load packages: %s", paste0(packages[!ok], collapse = ", "))

  ok = vlapply(namespaces, requireNamespace)
  if (!all(ok))
    stopf("Failed to load namespaces: %s", paste0(namespaces[!ok], collapse = ", "))

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

  invisible(TRUE)
}
