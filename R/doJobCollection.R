#' @title Execute Jobs of a JobCollection
#'
#' @description
#' Executes every job in a \code{\link{JobCollection}}.
#' This function is intended to be called on the slave.
#'
#' @param jc [\code{\link{JobCollection}}]\cr
#'   Either an object of class \dQuote{JobCollection} as returned by
#'   \code{\link{makeJobCollection}} or a string point to file containing a
#'   \dQuote{JobCollection} (saved with \code{\link[base]{saveRDS}}).
#' @param con [\code{\link[base]{connection}} | \code{character(1)}]\cr
#'   A connection for the output. Defaults to \code{\link[base]{stdout}}.
#'   Alternatively the name of a file to write to.
#' @return [\code{character(1)}]: Hash of the \code{\link{JobCollection}} executed.
#' @export
doJobCollection = function(jc, con = stdout()) {
  UseMethod("doJobCollection")
}


#' @export
doJobCollection.character = function(jc, con = stdout()) {
  obj = readRDS(jc)
  if (!obj$debug)
    file.remove(jc)
  doJobCollection.JobCollection(obj, con = con)
}


#' @export
doJobCollection.JobCollection = function(jc, con = stdout()) {
  error = function(msg, ...) {
    updates = data.table(job.id = jc$defs$job.id, started = ustamp(), done = ustamp(),
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

  # sink output
  if (!inherits(con, "terminal")) {
    close.sinks = function() {
      sink(type = "message")
      sink(type = "output")
      if (close.file)
        close(con)
    }

    if (!inherits(con, "connection")) {
      con = file(con, open = "wt")
      close.file = TRUE
    } else {
      close.file = FALSE
    }

    sink(file = con)
    sink(file = con, type = "message")
    on.exit(close.sinks(), add = TRUE)
  }


  # subset array jobs
  if (isTRUE(jc$resources$chunks.as.arrayjobs) && !is.na(jc$array.var)) {
    i = Sys.getenv(jc$array.var)
    if (nzchar(i)) {
      i = as.integer(i)
      if (!testInteger(i, any.missing = FALSE, lower = 1L, upper = nrow(jc$defs)))
        return(error("Failed to subset JobCollection using array environment variable '%s' [='%s']", jc$array.var, i))
      jc$defs = jc$defs[i]
    }
  }

  # say hi
  n.jobs = nrow(jc$defs)
  s = now()
  catf("### [bt %s]: Starting calculation of %i jobs", s, n.jobs)
  catf("### [bt %s]: Setting working directory to '%s'", s, jc$work.dir)

  # set work dir
  if (!dir.exists(jc$work.dir))
    return(error("Work dir does not exist"))
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd), add = TRUE)

  # setup inner parallelization
  if (!is.null(jc$resources$pm.backend)) {
    if (!requireNamespace("parallelMap", quietly = TRUE))
      return(error("parallelMap not installed"))
    pm.opts = filterNull(list(mode = jc$resources$pm.backend, cpus = jc$resources$ncpus, level = jc$resources$pm.level, show.info = FALSE))
    do.call(parallelMap::parallelStart, pm.opts)
    on.exit(parallelMap::parallelStop(), add = TRUE)
    pm.opts = parallelMap::parallelGetOptions()$settings
    catf("### [bt %s]: Using %i CPUs for parallelMap/%s on level '%s'", s, pm.opts$cpus, pm.opts$mode, if (is.na(pm.opts$level)) "default" else pm.opts$level)
  }

  measure.memory = isTRUE(jc$resources$measure.memory)
  catf("### [bt %s]: Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"))

  # try to pre-fetch some objects from the file system and load registry dependencies
  cache = Cache$new(jc$file.dir)
  ok = try(loadRegistryDependencies(jc, switch.wd = FALSE), silent = TRUE)
  if (is.error(ok))
    return(error("Error loading registry dependencies: %s", as.character(ok)))
  buf = UpdateBuffer$new(jc$defs$job.id)

  runHook(jc, "pre.do.collection", cache = cache)

  for (i in seq_len(n.jobs)) {
    job = getJob(jc, jc$defs$job.id[i], cache = cache)
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

  buf$flush(jc, force = TRUE)

  catf("### [bt %s]: Calculation finished!", now())
  runHook(jc, "post.do.collection", cache = cache)
  invisible(jc$job.hash)
}


UpdateBuffer = R6Class("UpdateBuffer",
  cloneable = FALSE,
  public = list(
    updates = NULL,
    next.update = NA_integer_,
    initialize = function(ids) {
      self$updates = data.table(job.id = ids, started = NA_integer_, done = NA_integer_, error = NA_character_, memory = NA_real_, written = 0L, key = "job.id")
      self$next.update = ustamp() + as.integer(runif(1L, 300, 1800))
    },

    add = function(id, x) {
      self$updates[list(id), names(x) := x]
    },

    flush = function(jc, force = FALSE) {
      if (force || ustamp() > self$next.update) {
        i = self$updates[!is.na(started) & written == 0L, which = TRUE]
        if (length(i) > 0L) {
          count = max(self$updates$written) + 1L
          writeRDS(self$updates[i, !"written", with = FALSE], file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE)
          self$updates[i, "written" := count]
          self$next.update = ustamp() + as.integer(runif(1L, 300, 1800))
        }
      }
    }
  )
)
