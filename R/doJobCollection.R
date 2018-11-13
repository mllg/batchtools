#' @title Execute Jobs of a JobCollection
#'
#' @description
#' Executes every job in a \code{\link{JobCollection}}.
#' This function is intended to be called on the slave.
#'
#' @param jc [\code{\link{JobCollection}}]\cr
#'   Either an object of class \dQuote{JobCollection} as returned by
#'   \code{\link{makeJobCollection}} or a string with the path to file
#'   containing a \dQuote{JobCollection} as RDS file (as stored by \code{\link{submitJobs}}).
#' @param output [\code{character(1)}]\cr
#'   Path to a file to write the output to. Defaults to \code{NULL} which means
#'   that output is written to the active \code{\link[base]{sink}}.
#'   Do not set this if your scheduler redirects output to a log file.
#' @return [\code{character(1)}]: Hash of the \code{\link{JobCollection}} executed.
#' @family JobCollection
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(1) }
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:2, reg = tmp)
#' jc = makeJobCollection(1:2, reg = tmp)
#' doJobCollection(jc)
doJobCollection = function(jc, output = NULL) {
  UseMethod("doJobCollection")
}


#' @export
doJobCollection.character = function(jc, output = NULL) {
  obj = readRDS(jc)
  force(obj)
  if (!batchtools$debug && !obj$array.jobs) {
    fs::file_delete(jc)
    job = fs::path_ext_set(jc, "job")
    if (fs::file_exists(job))
      fs::file_delete(job)
  }
  doJobCollection.JobCollection(obj, output = output)
}


#' @export
doJobCollection.JobCollection = function(jc, output = NULL) {
  error = function(msg, ...) {
    now = ustamp()
    updates = data.table(job.id = jc$jobs$job.id, started = now, done = now,
      error = stri_trunc(stri_trim_both(sprintf(msg, ...)), 500L, " [truncated]"),
      mem.used = NA_real_, key = "job.id")
    writeRDS(updates, file = fs::path(jc$file.dir, "updates", sprintf("%s.rds", jc$job.hash)))
    invisible(NULL)
  }

  # signal warnings immediately
  opts = options("warn")
  options(warn = 1L)
  on.exit(options(opts))

  # setup output connection
  if (!is.null(output)) {
    if (!testPathForOutput(output, overwrite = TRUE))
      return(error("Cannot create output file for logging"))
    fp = file(output, open = "wt")
    sink(file = fp)
    sink(file = fp, type = "message")
    on.exit({ sink(type = "message"); sink(type = "output"); close(fp) }, add = TRUE)
  }

  # subset array jobs
  if (jc$array.jobs) {
    i = as.integer(Sys.getenv(jc$array.var))
    if (!testInteger(i, any.missing = FALSE, lower = 1L, upper = nrow(jc$jobs)))
      return(error("Failed to subset JobCollection using array environment variable '%s' [='%s']", jc$array.var, i))
    jc$jobs = jc$jobs[i]
  }

  # say hi
  n.jobs = nrow(jc$jobs)
  s = now()
  catf("### [bt%s]: This is batchtools v%s", s, packageVersion("batchtools"))
  catf("### [bt%s]: Starting calculation of %i jobs", s, n.jobs)
  catf("### [bt%s]: Setting working directory to '%s'", s, jc$work.dir)

  # set work dir
  if (!fs::dir_exists(jc$work.dir))
    return(error("Work dir does not exist"))
  local_dir(jc$work.dir)

  # load registry dependencies: packages, source files, ...
  # note that this should happen _before_ parallelMap or foreach is initialized
  ok = try(loadRegistryDependencies(jc, must.work = TRUE), silent = TRUE)
  if (is.error(ok))
    return(error("Error loading registry dependencies: %s", as.character(ok)))

  # setup inner parallelization with parallelMap
  if (hasName(jc$resources, "pm.backend")) {
    if (!requireNamespace("parallelMap", quietly = TRUE))
      return(error("parallelMap not installed"))
    pm.opts = filterNull(insert(list(mode = jc$resources$pm.backend, cpus = jc$resources$ncpus, show.info = FALSE), jc$resources$pm.opts))
    do.call(parallelMap::parallelStart, pm.opts)
    on.exit(parallelMap::parallelStop(), add = TRUE)
    pm.opts = parallelMap::parallelGetOptions()$settings
    catf("### [bt%s]: Using %i CPUs for parallelMap/%s on level '%s'", s, pm.opts$cpus, pm.opts$mode, if (is.na(pm.opts$level)) "default" else pm.opts$level)
  }

  # setup inner parallelization with foreach
  if (hasName(jc$resources, "foreach.backend")) {
    if (!requireNamespace("foreach", quietly = TRUE))
      return(error("Package 'foreach' is not installed"))
    backend = jc$resources$foreach.backend
    ncpus = jc$resources$ncpus

    if (backend == "seq") {
      foreach::registerDoSEQ()
    } else if (backend == "parallel") {
      if (!requireNamespace("doParallel", quietly = TRUE))
        return(error("Package 'doParallel' is not installed"))
      doParallel::registerDoParallel(cores = ncpus)
    } else if (backend == "mpi") {
      if (!requireNamespace("doMPI", quietly = TRUE))
        return(error("Package 'doMPI' is not installed"))
      cl = doMPI::startMPIcluster(count = ncpus)
      doMPI::registerDoMPI(cl)
      on.exit(doMPI::closeCluster(cl), add = TRUE)
    } else {
      return(error("Unknwon foreach backend: '%s'", backend))
    }
    catf("### [bt%s]: Using %i CPUs for foreach/%s", s, ncpus, backend)
  }

  # setup memory measurement
  measure.memory = isTRUE(jc$resources$measure.memory)
  catf("### [bt%s]: Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"))
  if (measure.memory) {
    memory.mult = c(if (.Machine$sizeof.pointer == 4L) 28L else 56L, 8L)
  }

  # try to pre-fetch some objects from the file system
  reader = RDSReader$new(n.jobs > 1L)
  buf = UpdateBuffer$new(jc$jobs$job.id)

  runHook(jc, "pre.do.collection", reader = reader)

  for (i in seq_len(n.jobs)) {
    job = getJob(jc, i, reader = reader)
    id = job$id

    update = list(started = ustamp(), done = NA_integer_, error = NA_character_, mem.used = NA_real_)
    catf("### [bt%s]: Starting job [batchtools job.id=%i]", now(), id)
    if (measure.memory) {
      gc(reset = TRUE)
      result = try(execJob(job))
      update$mem.used = sum(gc()[, 6L] * memory.mult)
    } else {
      result = try(execJob(job))
    }
    update$done = ustamp()

    if (is.error(result)) {
      catf("\n### [bt%s]: Job terminated with an exception [batchtools job.id=%i]", now(), id)
      update$error = stri_trunc(stri_trim_both(as.character(result)), 500L, " [truncated]")
    } else {
      catf("\n### [bt%s]: Job terminated successfully [batchtools job.id=%i]", now(), id)
      writeRDS(result, file = getResultFiles(jc, id))
    }
    buf$add(i, update)
    buf$flush(jc)
  }

  runHook(jc, "post.do.collection", updates = buf$updates, reader = reader)
  buf$save(jc)
  catf("### [bt%s]: Calculation finished!", now())

  invisible(jc$job.hash)
}


UpdateBuffer = R6Class("UpdateBuffer",
  cloneable = FALSE,
  public = list(
    updates = NULL,
    next.update = NA_real_,
    initialize = function(ids) {
      self$updates = data.table(job.id = ids, started = NA_real_, done = NA_real_, error = NA_character_, mem.used = NA_real_, written = FALSE, key = "job.id")
      self$next.update = Sys.time() + runif(1L, 60, 300)
    },

    add = function(i, x) {
      set(self$updates, i, names(x), x)
    },

    save = function(jc) {
      i = self$updates[!is.na(started) & (!written), which = TRUE]
      if (length(i) > 0L) {
        first.id = self$updates$job.id[i[1L]]
        writeRDS(self$updates[i, !"written"], file = fs::path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, first.id)))
        set(self$updates, i, "written", TRUE)
      }
    },

    flush = function(jc) {
      now = Sys.time()
      if (now > self$next.update) {
        self$save(jc)
        self$next.update = now + runif(1L, 60, 300)
      }
    }

  )
)
