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
#'   \item{\code{sleep}:}{Custom sleep function. See \code{\link{waitForJobs}}.}
#'   \item{\code{expire.after}:}{Number of iterations before treating jobs as expired in \code{\link{waitForJobs}}.}
#'   \item{\code{compress}:}{Compression algorithm to use via \code{\link{saveRDS}}.}
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
#'   In the configuration file you can define how \pkg{batchtools} interacts with the system via \code{\link{ClusterFunctions}}.
#'   Separating the configuration of the underlying host system from the R code allows to easily move computation to another site.
#'
#'   The file lookup is implemented in the internal (but exported) function \code{findConfFile} which returns the first file found of the following candidates:
#'   \enumerate{
#'    \item{File \dQuote{batchtools.conf.R} in the path specified by the environment variable \dQuote{R_BATCHTOOLS_SEARCH_PATH}.}
#'    \item{File \dQuote{batchtools.conf.R} in the current working directory.}
#'    \item{File \dQuote{config.R} in the user configuration directory as reported by \code{rappdirs::user_config_dir("batchtools", expand = FALSE)} (depending on OS, e.g., on linux this usually resolves to \dQuote{~/.config/batchtools/config.R}).}
#'    \item{\dQuote{.batchtools.conf.R} in the home directory (\dQuote{~}).}
#'    \item{\dQuote{config.R} in the site config directory as reported by \code{rappdirs::site_config_dir("batchtools")} (depending on OS). This file can be used for admins to set sane defaults for a computation site.}
#'   }
#'   Set to \code{NA} if you want to suppress reading any configuration file.
#'   If a configuration file is found, it gets sourced inside the environment of the registry after the defaults for all variables are set.
#'   Therefore you can set and overwrite slots, e.g. \code{default.resources = list(walltime = 3600)} to set default resources or \dQuote{max.concurrent.jobs} to
#'   limit the number of jobs allowed to run simultaneously on the system.
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
#'   Default is a random integer between 1 and 32768
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
#'     \item{\code{hash} [character(1)]:}{Unique hash which changes each time the registry gets saved to the file system. Can be utilized to invalidate the cache of \pkg{knitr}.}
#'   }
#' @aliases Registry
#' @family Registry
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
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
  assertString(conf.file, na.ok = TRUE)
  assertCharacter(packages, any.missing = FALSE, min.chars = 1L)
  assertCharacter(namespaces, any.missing = FALSE, min.chars = 1L)
  assertCharacter(source, any.missing = FALSE, min.chars = 1L)
  assertCharacter(load, any.missing = FALSE, min.chars = 1L)
  assertFlag(make.default)
  seed = if (is.null(seed)) as.integer(runif(1L, 1, 32768)) else asCount(seed, positive = TRUE)

  reg = new.env(parent = asNamespace("batchtools"))
  reg$file.dir = file.dir
  reg$work.dir = work.dir
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
    mem.used    = double(0L),
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
    reg$file.dir = fs::file_temp("registry", tmp_dir = reg$temp.dir)
  "!DEBUG [makeRegistry]: Creating directories in '`reg$file.dir`'"

  fs::dir_create(c(reg$file.dir, reg$work.dir))
  reg$file.dir = fs::path_abs(reg$file.dir)
  reg$work.dir = fs::path_abs(reg$work.dir)

  fs::dir_create(fs::path(reg$file.dir, c("jobs", "results", "updates", "logs", "exports", "external")))
  with_dir(reg$work.dir, loadRegistryDependencies(reg))

  class(reg) = "Registry"
  saveRegistry(reg)
  reg$mtime = file_mtime(fs::path(reg$file.dir, "registry.rds"))
  reg$hash = rnd_hash()
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

#' @title assertRegistry
#'
#' @description
#' Assert that a given object is a \code{batchtools} registry.
#' Additionally can sync the registry, check if it is writeable, or check if jobs are running.
#' If any check fails, throws an error indicting the reason for the failure.
#'
#' @param reg [\code{\link{Registry}}]\cr
#'   The object asserted to be a \code{Registry}.
#' @param class [\code{character(1)}]\cr
#'   If \code{NULL} (default), \code{reg} must only inherit from class \dQuote{Registry}.
#'   Otherwise check that \code{reg} is of class \code{class}.
#'   E.g., if set to \dQuote{Registry}, a \code{\link{ExperimentRegistry}} would not pass.
#' @param writeable [\code{logical(1)}]\cr
#'   Check if the registry is writeable.
#' @param sync [\code{logical(1)}]\cr
#'   Try to synchronize the registry by including pending results from the file system.
#'   See \code{\link{syncRegistry}}.
#' @param running.ok [\code{logical(1)}]\cr
#'   If \code{FALSE} throw an error if jobs associated with the registry are currently running.
#' @return \code{TRUE} invisibly.
#' @export
assertRegistry = function(reg, class = NULL, writeable = FALSE, sync = FALSE, running.ok = TRUE) {
  if (batchtools$debug) {
    if (!identical(key(reg$status), "job.id"))
      stop("Key of reg$job.id lost")
    if (!identical(key(reg$defs), "def.id"))
      stop("Key of reg$defs lost")
    if (!identical(key(reg$resources), "resource.id"))
      stop("Key of reg$resources lost")
  }

  if (is.null(class)) {
    assertClass(reg, "Registry")
  } else {
    assertString(class)
    assertClass(reg, class, ordered = TRUE)
  }
  assertFlag(writeable)
  assertFlag(sync)
  assertFlag(running.ok)

  if (reg$writeable && file_mtime(fs::path(reg$file.dir, "registry.rds")) > reg$mtime + 1) {
    warning("Registry has been altered since last read. Switching to read-only mode in this session. See ?loadRegistry.")
    reg$writeable = FALSE
  }

  if (writeable && !reg$writeable)
    stop("Registry must be writeable. See ?loadRegistry.")

  if (!running.ok && nrow(.findOnSystem(reg = reg)) > 0L)
    stop("This operation is not allowed while jobs are running on the system")

  if (sync) {
    merged = sync(reg)
    if (length(merged)) {
      saveRegistry(reg)
      file_remove(merged)
    }
  }

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

  path = fs::path(x$file.dir, "exports")
  fns = list.files(path, pattern = "\\.rds$")
  if (length(fns) > 0L) {
    ee = .GlobalEnv
    Map(function(name, fn) {
      delayedAssign(x = name, value = readRDS(fn), assign.env = ee)
    }, name = unmangle(fns), fn = fs::path(path, fns))
  }

  invisible(TRUE)
}
