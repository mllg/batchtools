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
  if (!inherits(con, "connection")) {
    con = file(con, open = "wt")
    on.exit(close(con))
  }

  # signal warnings immediately
  warn = getOption("warn")
  on.exit(options(warn = warn), add = TRUE)
  options(warn = 1L)

  # subset array jobs
  if (isTRUE(jc$resources$chunks.as.arrayjobs) && !is.na(jc$array.var)) {
    i = Sys.getenv(jc$array.var)
    if (nzchar(i)) {
      i = as.integer(i)
      if (!testInteger(i, any.missing = FALSE, lower = 1L, upper = nrow(jc$defs)))
        return(slaveError(jc, sprintf("Failed to subset JobCollection using array environment variable '%s' [='%s']", jc$array.var, i)))
      jc$defs = jc$defs[i]
    }
  }

  # say hi
  n.jobs = nrow(jc$defs)
  s = now()
  catf("[job(chunk): %s] Starting calculation of %i jobs", s, n.jobs, con = con)
  catf("[job(chunk): %s] Setting working directory to '%s'", s, jc$work.dir, con = con)

  # set work dir
  if (!dir.exists(jc$work.dir))
    return(slaveError(jc, "Work dir does not exist"))
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd), add = TRUE)

  # setup inner parallelization
  if (!is.null(jc$resources$pm.backend)) {
    if (!requireNamespace("parallelMap", quietly = TRUE))
      return(slaveError(jc, "parallelMap not installed"))
    pm.opts = filterNull(list(mode = jc$resources$pm.backend, cpus = jc$resources$ncpus, level = jc$resources$pm.level, show.info = FALSE))
    do.call(parallelMap::parallelStart, pm.opts)
    on.exit(parallelMap::parallelStop(), add = TRUE)
    pm.opts = parallelMap::parallelGetOptions()$settings
    catf("[job(chunk): %s] Using %i CPUs for parallelMap/%s", s, pm.opts$cpus, pm.opts$mode, con = con)
  }

  measure.memory = isTRUE(jc$resources$measure.memory)
  catf("[job(chunk): %s] Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"), con = con)

  # try to pre-fetch some objects from the file system and load registry dependencies
  cache = Cache$new(jc$file.dir)
  ok = try(loadRegistryDependencies(jc, switch.wd = FALSE), silent = TRUE)
  if (is.error(ok))
    return(slaveError(jc, sprintf("Error loading registry dependencies: %s", as.character(ok))))
  buf = UpdateBuffer$new(jc$defs$job.id)

  runHook(jc, "pre.do.collection", con = con, cache = cache)

  for (i in seq_len(n.jobs)) {
    job = getJob(jc, jc$defs$job.id[i], cache = cache)
    id = job$id

    catf("[job(%i): %s] Starting job with job.id=%i", id, now(), id, con = con)
    update = list(started = ustamp(), done = NA_integer_, error = NA_character_, memory = NA_real_)
    if (measure.memory) {
      gc(reset = TRUE)
      result = capture(execJob(job))
      update$memory = sum(gc()[, 6L])
    } else {
      result = capture(execJob(job))
    }
    update$done = ustamp()
    catf("[job(%i): %s] %s", id, now(), result$output, con = con)

    if (is.error(result$res)) {
      catf("[job(%i): %s] Job terminated with an exception", id, now(), con = con)
      update$error = stri_trunc(stri_trim_both(as.character(result$res)), 500L, " [truncated]")
    } else {
      catf("[job(%i): %s] Job terminated successfully", id, now(), con = con)
      writeRDS(result$res, file = file.path(jc$file.dir, "results", sprintf("%i.rds", id)))
    }
    buf$add(id, update)
    buf$flush(jc)
  }

  buf$flush(jc, force = TRUE)

  catf("[job(chunk): %s] Calculation finished!", now(), con = con)
  runHook(jc, "post.do.collection", con = con, cache = cache)
  invisible(jc$job.hash)
}


UpdateBuffer = R6Class("UpdateBuffer",
  cloneable = FALSE,
  public = list(
    updates = NULL,
    next.update = NA_integer_,
    initialize = function(ids) {
      self$updates = data.table(job.id = ids, started = NA_integer_, done = NA_integer_, error = NA_character_, memory = NA_real_, written = 0L, key = "job.id")
      self$next.update = ustamp() + as.integer(runif(1L, 300L, 1800L))
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
          self$next.update = ustamp() + as.integer(runif(1L, 300L, 1800L))
        }
      }
    }
  )
)

slaveError = function(jc, msg) {
  updates = data.table(job.id = jc$defs$job.id, started = ustamp(), done = ustamp(),
    error = stri_trunc(stri_trim_both(msg), 500L, " [truncated]"),
    memory = NA_real_, key = "job.id")
  writeRDS(updates, file = file.path(jc$file.dir, "updates", sprintf("%s-0.rds", jc$job.hash)), wait = TRUE)
  invisible(NULL)
}
