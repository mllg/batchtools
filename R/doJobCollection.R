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
#' @return [\code{character(1)}]: Hash of the \code{\link{JobCollection}} executed.
#' @family JobCollection
#' @export
#' @examples
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
  if (!batchtools$debug)
    file.remove(jc)
  doJobCollection.JobCollection(obj, output = output)
}


#' @export
doJobCollection.JobCollection = function(jc, output = NULL) {
  now = function() strftime(Sys.time())

  error = function(msg, ...) {
    updates = data.table(job.id = jc$jobs$job.id, started = ustamp(), done = ustamp(),
      error = stri_trunc(stri_trim_both(sprintf(msg, ...)), 500L, " [truncated]"),
      memory = NA_real_, key = "job.id")
    writeRDS(updates, file = file.path(jc$file.dir, "updates", sprintf("%s-0.rds", jc$job.hash)), wait = TRUE)
    invisible(NULL)
  }

  # signal warnings immediately
  warn = getOption("warn")
  if (!identical(warn, 1L)) {
    on.exit(options(warn = warn))
    options(warn = 1L)
  }

  # setup output connection
  if (!is.null(output)) {
    assertPathForOutput(output)
    fp = file(output, open = "wt")
    sink(file = fp)
    sink(file = fp, type = "message")
    on.exit({ sink(type = "message"); sink(type = "output"); close(fp) }, add = TRUE)
  }

  # subset array jobs
  if (isTRUE(jc$resources$chunks.as.arrayjobs) && !is.na(jc$array.var)) {
    i = Sys.getenv(jc$array.var)
    if (nzchar(i)) {
      i = as.integer(i)
      if (!testInteger(i, any.missing = FALSE, lower = 1L, upper = nrow(jc$jobs)))
        return(error("Failed to subset JobCollection using array environment variable '%s' [='%s']", jc$array.var, i))
      jc$jobs = jc$jobs[i]
    }
  }

  # say hi
  n.jobs = nrow(jc$jobs)
  s = now()
  catf("### [bt %s]: Starting calculation of %i jobs", s, n.jobs)
  catf("### [bt %s]: Setting working directory to '%s'", s, jc$work.dir)

  # set work dir
  if (!dir.exists(jc$work.dir))
    return(error("Work dir does not exist"))
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd), add = TRUE)

  # load registry dependencies: packages, source files, ...
  # note that this should happen _before_ parallelMap is initialized
  ok = try(loadRegistryDependencies(jc, switch.wd = FALSE), silent = TRUE)
  if (is.error(ok))
    return(error("Error loading registry dependencies: %s", as.character(ok)))

  # setup inner parallelization
  if (hasName(jc$resources, "pm.backend")) {
    if (!requireNamespace("parallelMap", quietly = TRUE))
      return(error("parallelMap not installed"))
    pm.opts = filterNull(insert(list(mode = jc$resources$pm.backend, cpus = jc$resources$ncpus, show.info = FALSE), jc$resources$pm.opts))
    do.call(parallelMap::parallelStart, pm.opts)
    on.exit(parallelMap::parallelStop(), add = TRUE)
    pm.opts = parallelMap::parallelGetOptions()$settings
    catf("### [bt %s]: Using %i CPUs for parallelMap/%s on level '%s'", s, pm.opts$cpus, pm.opts$mode, if (is.na(pm.opts$level)) "default" else pm.opts$level)
  }

  # setup memory measurement
  measure.memory = isTRUE(jc$resources$measure.memory)
  catf("### [bt %s]: Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"))

  # try to pre-fetch some objects from the file system
  cache = Cache$new(jc$file.dir)
  buf = UpdateBuffer$new(jc$jobs$job.id)

  runHook(jc, "pre.do.collection", cache = cache)

  for (i in seq_len(n.jobs)) {
    job = getJob(jc, jc$jobs[i], cache = cache)
    id = job$id

    catf("### [bt %s]: Starting job [batchtools job.id=%i]", now(), id)
    update = list(started = ustamp(), done = NA_integer_, error = NA_character_, memory = NA_real_)
    if (measure.memory) {
      gc(reset = TRUE)
      result = try(execJob(job))
      update$memory = sum(gc()[, 6L])
    } else {
      result = try(execJob(job))
    }
    update$done = ustamp()

    if (is.error(result)) {
      catf("\n### [bt %s]: Job terminated with an exception [batchtools job.id=%i]", now(), id)
      update$error = stri_trunc(stri_trim_both(as.character(result)), 500L, " [truncated]")
    } else {
      catf("\n### [bt %s]: Job terminated successfully [batchtools job.id=%i]", now(), id)
      writeRDS(result, file = file.path(jc$file.dir, "results", sprintf("%i.rds", id)))
    }
    buf$add(id, update)
    buf$flush(jc)
  }

  buf$save(jc)

  catf("### [bt %s]: Calculation finished!", now())
  runHook(jc, "post.do.collection", cache = cache)
  invisible(jc$job.hash)
}


UpdateBuffer = R6Class("UpdateBuffer",
  cloneable = FALSE,
  public = list(
    updates = NULL,
    next.update = NA_integer_,
    count = 0L,
    initialize = function(ids) {
      self$updates = data.table(job.id = ids, started = NA_integer_, done = NA_integer_, error = NA_character_, memory = NA_real_, written = FALSE, key = "job.id")
      self$next.update = ustamp() + as.integer(runif(1L, 300, 1800))
    },

    add = function(id, x) {
      self$updates[list(id), names(x) := x]
    },

    save = function(jc) {
      i = self$updates[!written & !is.na(started), which = TRUE]
      if (length(i) > 0L) {
        self$count = self$count + 1L
        writeRDS(self$updates[i, !"written", with = FALSE], file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, self$count)), wait = TRUE)
        self$updates[i, "written" := TRUE]
      }
    },

    flush = function(jc) {
      if (ustamp() > self$next.update) {
        self$save(jc)
        self$next.update = ustamp() + as.integer(runif(1L, 300, 1800))
      }
    }

  )
)
