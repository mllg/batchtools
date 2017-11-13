#' @title Registry Constructor
#'
#' @description
#' \code{makeRegistry} constructs the inter-communication object for all functions in \code{batchtools}.
#' All communication transactions are processed via the file system:
#' All information required to run a job is stored as \code{\link{JobCollection}} in a file in the
#' a subdirectory of the \code{file.dir} directory.
#' Each jobs stores its results as well as computational status information (start time, end time, error message, ...)
#' also on the file system which is regular merged parsed by the master using \code{\link{syncRegistry}}.
#' After integrating the new information into the Registry, the Registry is serialized to the file system via \code{\link{saveRegistry}}.
#' Both \code{\link{syncRegistry}} and \code{\link{saveRegistry}} are called whenever required internally.
#' Therefore it should be safe to quit the R session at any time.
#' Work can later be resumed by calling \code{\link{loadRegistry}} which de-serializes the registry from
#' the file system.
#'
#' The registry created last is saved in the package namespace (unless \code{make.default} is set to
#' \code{FALSE}) and can be retrieved via \code{\link{getDefaultRegistry}}.
#'
#' Canceled jobs and jobs submitted multiple times may leave stray files behind.
#' These can be swept using \code{\link{sweepRegistry}}.
#' \code{\link{clearRegistry}} completely erases all jobs from a registry, including log files and results,
#' and thus allows you to start over.
#'
#' @details
#' Currently \pkg{batchtools} understands the following options set via the configuration file:
#' \describe{
#'   \item{\code{cluster.functions}:}{As returned by a constructor, e.g. \code{\link{makeClusterFunctionsSlurm}}.}
#'   \item{\code{default.resources}:}{List of resources to use. Will be overruled by resources specified via \code{\link{submitJobs}}.}
#'   \item{\code{temp.dir}:}{Path to directory to use for temporary registries.}
#' }
#'
#' @param file.dir [\code{character(1)}]\cr
#'   Path where all files of the registry are saved.
#'   Default is directory \dQuote{registry} in the current working directory.
#'   The provided path will get normalized unless it is given relative to the home directory
#'   (i.e., starting with \dQuote{~}). Note that some templates do not handle relative paths well.
#'
#'   If you pass \code{NA}, a temporary directory will be used.
#'   This way, you can create disposable registries for \code{\link{btlapply}} or examples.
#'   By default, the temporary directory \code{\link[base]{tempdir}()} will be used.
#'   If you want to use another directory, e.g. a directory which is shared between nodes,
#'   you can set it in your configuration file by setting the variable \code{temp.dir}.
#' @param work.dir [\code{character(1)}]\cr
#'   Working directory for R process for running jobs.
#'   Defaults to the working directory currently set during Registry construction (see \code{\link[base]{getwd}}).
#'   \code{loadRegistry} uses the stored \code{work.dir}, but you may also explicitly overwrite it,
#'   e.g., after switching to another system.
#'
#'   The provided path will get normalized unless it is given relative to the home directory
#'   (i.e., starting with \dQuote{~}). Note that some templates do not handle relative paths well.
#' @param conf.file [\code{character(1)}]\cr
#'   Path to a configuration file which is sourced while the registry is created.
#'   For example, you can set cluster functions or default resources in it.
#'   The script is executed inside the environment of the registry after the defaults for all variables are set,
#'   thus you can set and overwrite slots, e.g. \code{default.resources = list(walltime = 3600)} to set default resources.
#'
#'   The file lookup defaults to a heuristic which first tries to read \dQuote{batchtools.conf.R} in the current working directory.
#'   If not found, it looks for a configuration file \dQuote{config.R} in the OS dependent user configuration directory
#'   as reported by via \code{rappdirs::user_config_dir("batchtools", expand = FALSE)} (e.g., on linux this
#'   usually resolves to \dQuote{~/.config/batchtools/config.R}).
#'   If this file is also not found, the heuristic finally tries to read the file \dQuote{.batchtools.conf.R} in the
#'   home directory (\dQuote{~}).
#'   Set to \code{character(0)} if you want to disable this heuristic.
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
#' @return [\code{environment}] of class \dQuote{Registry} with the following slots:
#'   \describe{
#'     \item{\code{file.dir} [path]:}{File directory.}
#'     \item{\code{work.dir} [path]:}{Working directory.}
#'     \item{\code{temp.dir} [path]:}{Temporary directory. Used if \code{file.dir} is \code{NA} to create temporary registries.}
#'     \item{\code{packages} [character()]:}{Packages to load on the slaves.}
#'     \item{\code{namespaces} [character()]:}{Namespaces to load on the slaves.}
#'     \item{\code{seed} [integer(1)]:}{Registry seed. Before each job is executed, the seed \code{seed + job.id} is set.}
#'     \item{\code{cluster.functions} [cluster.functions]:}{Usually set in your \code{conf.file}. Set via a call to \code{\link{makeClusterFunctions}}. See example.}
#'     \item{\code{default.resources} [named list()]:}{Usually set in your \code{conf.file}. Named list of default resources.}
#'     \item{\code{max.concurrent.jobs} [integer(1)]:}{Usually set in your \code{conf.file}. Maximum number of concurrent jobs for a single user and current registry on the system.
#'       \code{\link{submitJobs}} will try to respect this setting. The resource \dQuote{max.concurrent.jobs} has higher precedence.}
#'     \item{\code{defs} [data.table]:}{Table with job definitions (i.e. parameters).}
#'     \item{\code{status} [data.table]:}{Table holding information about the computational status. Also see \code{\link{getJobStatus}}.}
#'     \item{\code{resources} [data.table]:}{Table holding information about the computational resources used for the job. Also see \code{\link{getJobResources}}.}
#'     \item{\code{tags} [data.table]:}{Table holding information about tags. See \link{Tags}.}
#'   }
#' @aliases Registry
#' @family Registry
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' print(tmp)
#'
#' # Set cluster functions to interactive mode and start jobs in external R sessions
#' tmp$cluster.functions = makeClusterFunctionsInteractive(external = TRUE)
#'
#' # Change packages to load
#' tmp$packages = c("MASS")
#' saveRegistry(reg = tmp)
makeRegistry = function(file.dir = "registry", work.dir = getwd(), conf.file = findConfFile(), packages = character(0L), namespaces = character(0L),
  source = character(0L), load = character(0L), seed = NULL, make.default = TRUE) {
  assertString(file.dir, na.ok = TRUE)
  if (!is.na(file.dir))
    assertPathForOutput(file.dir, overwrite = FALSE)
  assertString(work.dir)
  assertDirectoryExists(work.dir, access = "r")
  assertCharacter(conf.file, any.missing = FALSE, max.len = 1L)
  assertCharacter(packages, any.missing = FALSE, min.chars = 1L)
  assertCharacter(namespaces, any.missing = FALSE, min.chars = 1L)
  assertCharacter(source, any.missing = FALSE, min.chars = 1L)
  assertCharacter(load, any.missing = FALSE, min.chars = 1L)
  assertFlag(make.default)
  seed = if (is.null(seed)) as.integer(runif(1L, 0, 32768)) else asCount(seed, positive = TRUE)

  reg = new.env(parent = asNamespace("batchtools"))
  reg$file.dir = file.dir
  reg$work.dir = npath(work.dir)
  reg$packages = packages
  reg$namespaces = namespaces
  reg$source = source
  reg$load = load
  reg$seed = seed
  reg$writeable = TRUE
  reg$version = packageVersion("batchtools")

  reg$defs = data.table(
    def.id    = integer(0L),
    job.pars  = list(),
    key       = "def.id")

  reg$status = data.table(
    job.id      = integer(0L),
    def.id      = integer(0L),
    submitted   = double(0L),
    started     = double(0L),
    done        = double(0L),
    error       = character(0L),
    memory      = double(0L),
    resource.id = integer(0L),
    batch.id    = character(0L),
    log.file    = character(0L),
    job.hash    = character(0L),
    job.name    = character(0L),
    key         = "job.id")

  reg$resources = data.table(
    resource.id   = integer(0L),
    resource.hash = character(0L),
    resources     = list(),
    key           = "resource.id")

  reg$tags = data.table(
    job.id = integer(0L),
    tag    = character(0L),
    key    = "job.id")

  setSystemConf(reg, conf.file)

  if (is.na(file.dir))
    reg$file.dir = tempfile("registry", tmpdir = reg$temp.dir)
  "!DEBUG [makeRegistry]: Creating directories in '`reg$file.dir`'"
  for (d in fp(reg$file.dir, c("jobs", "results", "updates", "logs", "exports", "external")))
    dir.create(d, recursive = TRUE)
  reg$file.dir = npath(reg$file.dir)

  with_dir(reg$work.dir, loadRegistryDependencies(reg))

  class(reg) = "Registry"
  saveRegistry(reg)
  reg$mtime = file.mtime(fp(reg$file.dir, "registry.rds"))
  info("Created registry in '%s' using cluster functions '%s'", reg$file.dir, reg$cluster.functions$name)
  if (make.default)
    batchtools$default.registry = reg
  return(reg)
}

#' @export
print.Registry = function(x, ...) {
  cat("Job Registry\n")
  catf("  Backend  : %s", x$cluster.functions$name)
  catf("  File dir : %s", x$file.dir)
  catf("  Work dir : %s", x$work.dir)
  catf("  Jobs     : %i", nrow(x$status))
  catf("  Seed     : %i", x$seed)
  catf("  Writeable: %s", x$writeable)
}

assertRegistry = function(reg, writeable = FALSE, sync = FALSE, strict = FALSE, running.ok = TRUE) {
  assertClass(reg, "Registry", ordered = strict)
  if (batchtools$debug) {
    if (!identical(key(reg$status), "job.id"))
      stop("Key of reg$job.id lost")
    if (!identical(key(reg$defs), "def.id"))
      stop("Key of reg$defs lost")
    if (!identical(key(reg$resources), "resource.id"))
      stop("Key of reg$resources lost")
  }


  if (reg$writeable && !identical(reg$mtime, file.mtime(fp(reg$file.dir, "registry.rds")))) {
    warning("Registry has been altered since last read. Switching to read-only mode in this session.")
    reg$writeable = FALSE
  }

  if (writeable && !reg$writeable)
    stop("Registry must be writeable")

  if (sync || !running.ok) {
    if (sync(reg))
      saveRegistry(reg)
  }

  if (!running.ok && nrow(.findOnSystem(reg = reg)) > 0L)
    stop("This operation is not allowed while jobs are running on the system")
  invisible(TRUE)
}

loadRegistryDependencies = function(x, must.work = FALSE) {
  "!DEBUG [loadRegistryDependencies]: Starting ..."
  pkgs = union(x$packages, "methods")
  handler = if (must.work) stopf else warningf
  ok = vlapply(pkgs, require, character.only = TRUE)
  if (!all(ok))
    handler("Failed to load packages: %s", stri_flatten(pkgs[!ok], ", "))

  ok = vlapply(x$namespaces, requireNamespace)
  if (!all(ok))
    handler("Failed to load namespaces: %s", stri_flatten(x$namespaces[!ok], ", "))

  if (length(x$source) > 0L) {
    for (fn in x$source) {
      ok = try(sys.source(fn, envir = .GlobalEnv), silent = TRUE)
      if (is.error(ok))
        handler("Failed to source file '%s': %s", fn, as.character(ok))
    }
  }

  if (length(x$load) > 0L) {
    for (fn in x$load) {
      ok = try(load(fn, envir = .GlobalEnv), silent = TRUE)
      if (is.error(ok))
        handler("Failed to load file '%s': %s", fn, as.character(ok))
    }
  }

  path = fp(x$file.dir, "exports")
  fns = list.files(path, pattern = "\\.rds$")
  if (length(fns) > 0L) {
    ee = .GlobalEnv
    Map(function(name, fn) {
      assign(x = name, value = readRDS(fn), envir = ee)
    }, name = unmangle(fns), fn = fp(path, fns))
  }

  invisible(TRUE)
}
