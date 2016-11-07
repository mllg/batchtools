#' @title Registry Constructor
#'
#' @description
#' \code{makeRegistry} constructs the inter-communication object for all functions in \code{batchtools}.
#' All communication transactions are processed via the file system:
#' All information required to run a job is stored as \code{\link{JobCollection}} in a file in the
#' a subdirectory of the \code{file.dir} directory.
#' Each jobs stores its results as well as computational status information (start time, end time, error message, ...)
#' also on the file system which is regular merged parsed by the master using \code{syncRegistry}.
#' After integrating the new information into the Registry, the Registry is serialized to the file system via \code{saveRegistry}.
#' Both \code{syncRegistry} and \code{saveRegistry} are called whenever required internally.
#' Therefore it should be safe to quit the R session at any time.
#' Work can later be resumed by calling \code{loadRegistry} which de-serializes the registry from
#' the file system.
#'
#' The registry created last is saved in the package namespace (unless \code{make.default} is set to
#' \code{FALSE}) and can be retrieved via \code{getDefaultRegistry}.
#'
#' Canceled jobs and repeatedly submitted jobs may leave stray files behind.
#' These can be swept using \code{sweepRegistry}.
#' \code{clearRegistry} completely erases all jobs from a registry, including log files and results,
#' and thus allows you to start over.
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
#' @return \code{makeRegistry}, \code{loadRegistry}, \code{getDefaultRegistry} and \code{setDefaultRegistry}
#'   return an environment of class \dQuote{Registry} with the following slots:
#'   \describe{
#'     \item{\code{file.dir} [path]:}{File directory.}
#'     \item{\code{work.dir} [path]:}{Working directory.}
#'     \item{\code{temp.dir} [path]:}{Temporary directory. Used if \code{file.dir} is \code{NA}.}
#'     \item{\code{packages} [character()]:}{Packages to load on the slaves.}
#'     \item{\code{namespaces} [character()]:}{Namespaces to load on the slaves.}
#'     \item{\code{seed} [integer(1)]:}{Registry seed. Before each job is executed, the seed \code{seed + job.id} is set.}
#'     \item{\code{cluster.functions} [cluster.functions]:}{Usually set in your \code{conf.file}. Set via a call to \code{\link{makeClusterFunctions}}. See example.}
#'     \item{\code{default.resources} [named list()]:}{Usually set in your \code{conf.file}. Named list of default resources.}
#'     \item{\code{max.concurrent.jobs} [integer(1)]:}{Usually set in your \code{conf.file}. Maximum number of concurrent jobs for a single user and current registry on the system.
#'       \code{\link{submitJobs}} will try to respect this setting.}
#'     \item{\code{defs} [data.table]:}{Table with job definitions (i.e. parameters).}
#'     \item{\code{status} [data.table]:}{Table holding information about the computational status. Also see \code{\link{getJobStatus}}.}
#'     \item{\code{resources} [data.table]:}{Table holding information about the computational resources used for the job. Also see \code{\link{getJobResources}}.}
#'     \item{\code{tags} [data.table]:}{Table holding information about tags. See \link{Tags}.}
#'   }
#'   The other functions \code{saveRegistry}, \code{syncRegistry}, \code{sweepRegistry} and \code{clearRegistry} return \code{TRUE}
#'   if the registry has been altered and successfully stored on the file system.
#' @aliases Registry
#' @name Registry
#' @rdname Registry
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
  seed = if (is.null(seed)) as.integer(runif(1L, 1, .Machine$integer.max / 2L)) else asCount(seed, positive = TRUE)

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
    pars      = list(),
    key       = "def.id")

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
  "!DEBUG Creating directories in '`reg$file.dir`'"
  for (d in file.path(reg$file.dir, c("jobs", "results", "updates", "logs", "exports", "external")))
    dir.create(d, recursive = TRUE)
  reg$file.dir = npath(reg$file.dir)

  loadRegistryDependencies(list(file.dir = file.dir, work.dir = work.dir, packages = packages, namespaces = namespaces, source = source, load = load), switch.wd = TRUE)

  class(reg) = "Registry"
  saveRegistry(reg)
  if (make.default)
    batchtools$default.registry = reg
  return(reg)
}

setSystemConf = function(reg, conf.file) {
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$default.resources = list()
  reg$temp.dir = tempdir()

  if (length(conf.file) > 0L) {
    assertString(conf.file)
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)

    assertClass(reg$cluster.functions, "ClusterFunctions")
    assertList(reg$default.resources, names = "unique")
    assertDirectoryExists(reg$temp.dir, access = "w")
  }
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
  if (!is.null(reg))
    assertRegistry(reg)
  batchtools$default.registry = reg
}

#' @export
print.Registry = function(x, ...) {
  cat("Job Registry\n")
  catf("  Name    : %s", x$cluster.functions$name)
  catf("  File dir: %s", x$file.dir)
  catf("  Work dir: %s", x$work.dir)
  catf("  Jobs    : %i", nrow(x$status))
  catf("  Seed    : %i", x$seed)
}

#' @export
#' @param update.paths [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the \code{file.dir} and \code{work.dir} will be updated in the registry. Note that this is
#'   likely to break computation on the system! Only do this if no jobs are currently running. Default is \code{FALSE}.
#'   If the provided \code{file.dir} does not match the stored \code{file.dir}, \code{loadRegistry} will return a
#'   registry in read-only mode.
#' @rdname Registry
loadRegistry = function(file.dir = getwd(), work.dir = NULL, conf.file = findConfFile(), make.default = TRUE, update.paths = FALSE) {
  assertString(file.dir)
  assertFlag(make.default)
  assertFlag(update.paths)

  readRegistry = function() {
    fn.old = file.path(file.dir, "registry.rds")
    fn.new = file.path(file.dir, "registry.new.rds")

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
  if (is.data.table(reg$tags)) # FIXME: remove on release
    alloc.col(reg$tags, ncol(reg$tags))
  else
    reg$tags = data.table(job.id = integer(0L), tag = character(0L), key = "job.id")

  file.dir = npath(file.dir)
  if (!update.paths) {
    before = npath(reg$file.dir, must.work = FALSE)
    if (before != file.dir) {
      warningf("The absolute path of the file.dir has changed (before '%s', now '%s'). Enabling read-only mode for the registry.", before, file.dir)
      reg$writeable = FALSE
    }
  }
  reg$file.dir = file.dir

  if (!is.null(work.dir)) {
    assertString(work.dir)
    reg$work.dir = npath(work.dir)
  }

  wd.exists = dir.exists(reg$work.dir)
  if (!wd.exists)
    warningf("The work.dir '%s' does not exist, jobs might fail to run on this system.", reg$work.dir)
  loadRegistryDependencies(reg, switch.wd = wd.exists)
  reg$cluster.functions = makeClusterFunctionsInteractive()
  setSystemConf(reg, conf.file)
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
    "!DEBUG Saving Registry"

    fn = file.path(reg$file.dir, c("registry.new.rds", "registry.rds"))
    ee = new.env(parent = asNamespace("batchtools"))
    list2env(mget(setdiff(ls(reg), c("cluster.functions", "default.resources", "temp.dir")), reg), ee)
    class(ee) = class(reg)
    writeRDS(ee, file = fn[1L], wait = TRUE)
    file.rename(fn[1L], fn[2L])
  } else {
    "!DEBUG Skipping saveRegistry (read-only)"
  }
  invisible(reg$writeable)
}

#' @rdname Registry
#' @export
sweepRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE, writeable = TRUE)
  store = FALSE
  "!DEBUG Running sweepRegistry"

  result.files = list.files(file.path(reg$file.dir, "results"), pattern = "\\.rds$")
  i = which(as.integer(stri_replace_last_fixed(result.files, ".rds", "")) %nin% .findSubmitted(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i obsolete result files", length(i))
    file.remove(file.path(reg$file.dir, "results", result.files[i]))
  }

  log.files = list.files(file.path(reg$file.dir, "logs"), pattern = "\\.log$")
  i = which(stri_replace_last_fixed(log.files, ".log", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete log files", length(i))
    file.remove(file.path(reg$file.dir, "logs", log.files[i]))
  }

  job.files = list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.rds$")
  i = which(stri_replace_last_fixed(job.files, ".rds", "") %nin% reg$status$job.hash)
  if (length(i) > 0L) {
    info("Removing %i obsolete job files", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.files[i]))
  }

  job.desc.files = list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.job$")
  if (length(job.desc.files) > 0L) {
    info("Removing %i job description files", length(i))
    file.remove(file.path(reg$file.dir, "jobs", job.desc.files))
  }

  external.dirs = list.files(file.path(reg$file.dir, "external"), pattern = "^[0-9]+$")
  i = which(as.integer(external.dirs) %nin% .findSubmitted(reg = reg)$job.id)
  if (length(i) > 0L) {
    info("Removing %i external directories of unsubmitted jobs", length(i))
    unlink(file.path(reg$file.dir, "external", external.dirs[i]), recursive = TRUE)
  }

  i = reg$resources[!reg$status, on = "resource.id", which = TRUE]
  if (length(i) > 0L) {
    info("Removing %i resource specifications", length(i))
    reg$resources = reg$resources[-i]
    store = TRUE
  }

  i = reg$tags[!reg$status, on = "job.id", which = TRUE]
  if (length(i) > 0L) {
    info("Removing %i tags", length(i))
    reg$tags = reg$tags[-i]
    store = TRUE
  }

  if (store) saveRegistry(reg) else FALSE
}


#' @rdname Registry
#' @export
clearRegistry = function(reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, running.ok = FALSE, sync = TRUE)
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
  "!DEBUG Loading Registry dependencies"
  pkgs = union(x$packages, "methods")
  ok = vlapply(pkgs, require, character.only = TRUE)
  if (!all(ok))
    stopf("Failed to load packages: %s", stri_join(pkgs[!ok], collapse = ", "))

  ok = vlapply(x$namespaces, requireNamespace)
  if (!all(ok))
    stopf("Failed to load namespaces: %s", stri_join(x$namespaces[!ok], collapse = ", "))

  if (switch.wd) {
    wd = getwd()
    on.exit(setwd(wd))
    setwd(x$work.dir)
  }

  if (length(x$source) > 0L) {
    for (fn in x$source) {
      sys.source(fn, envir = .GlobalEnv)
      if (is.error(ok))
        stopf("Error sourcing file '%s': %s", fn, as.character(ok))
    }
  }

  if (length(x$load) > 0L) {
    for (fn in x$load) {
      ok = try(load(fn, envir = .GlobalEnv), silent = TRUE)
      if (is.error(ok))
        stopf("Error loading file '%s': %s", fn, as.character(ok))
    }
  }

  path = file.path(x$file.dir, "exports")
  fns = list.files(path, pattern = "\\.rds$")
  if (length(fns) > 0L) {
    ee = .GlobalEnv
    Map(function(name, fn) {
      assign(x = name, value = readRDS(fn), envir = ee)
    }, name = basename(stri_sub(fns, to = -5L)), fn = file.path(path, fns))
  }

  invisible(TRUE)
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
  if (reg$writeable) {
    if (sync)
      syncRegistry(reg)
  } else {
    if (writeable)
      stop("Registry must be writeable")
  }
  if (!running.ok && nrow(.findOnSystem(reg = reg)) > 0L)
    stop("This operation is not allowed while jobs are running on the system")
  invisible(TRUE)
}

#' @rdname Registry
#' @export
syncRegistry = function(reg = getDefaultRegistry()) {
  "!DEBUG Triggered syncRegistry"
  fns = list.files(file.path(reg$file.dir, "updates"), full.names = TRUE)
  if (length(fns) == 0L)
    return(invisible(TRUE))

  if (reg$writeable) {
    info("Syncing %i files ...", length(fns))
  } else {
    info("Skipping %i updates in read-only mode ...", length(fns))
    return(invisible(FALSE))
  }

  runHook(reg, "pre.sync", fns = fns)

  updates = lapply(fns, function(fn) {
    x = try(readRDS(fn), silent = TRUE)
    if (is.error(x)) NULL else x
  })

  failed = vlapply(updates, is.null)
  updates = rbindlist(updates)

  if (nrow(updates) > 0L) {
    expr = quote(`:=`(started = i.started, done = i.done, error = i.error, memory = i.memory))
    reg$status[updates, eval(expr), on = "job.id"]
    saveRegistry(reg)
    unlink(fns[!failed])
  }

  runHook(reg, "post.sync", updates = updates)
  invisible(TRUE)
}

findConfFile = function() {
  x = "batchtools.conf.R"
  if (file.exists(x))
    return(npath(x))

  x = file.path(user_config_dir("batchtools", expand = FALSE), "config.R")
  if (file.exists(x))
    return(x)

  x = npath(file.path("~", ".batchtools.conf.R"), must.work = FALSE)
  if (file.exists(x))
    return(npath(x))

  return(character(0L))
}
