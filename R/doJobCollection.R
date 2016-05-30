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
#' @return [\code{NULL}].
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

  # throw warning immediately
  warn = getOption("warn")
  on.exit(options(warn = warn), add = TRUE)
  options(warn = 1L)

  # say hi
  n.jobs = nrow(jc$defs)
  s = now()
  catf("[job(chunk): %s] Starting calculation of %i jobs", s, n.jobs, con = con)
  catf("[job(chunk): %s] Setting working directory to '%s'", s, jc$work.dir, con = con)

  # set work dir
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd), add = TRUE)

  # setup inner parallelization
  inner = getParallelMode(jc$resources$inner.mode, jc$resources$inner.backend, jc$resources$inner.ncpus, n.jobs)
  if (inner$mode != "none") {
    if (inner$mode == "chunk") {
      catf("[job(chunk): %s] Using %i CPUs for inner chunk parallelization", s, inner$ncpus, con = con)
      p = switch(inner$backend,
        "sequential" = Sequential$new(),
        "multicore" = Multicore$new(inner$ncpus),
        "socket" = Snow$new("socket", inner$ncpus),
        "mpi" = Snow$new("mpi", inner$ncpus))
    } else if (inner$mode == "pm") {
      loadNamespace("parallelMap")
      parallelMap::parallelStart(mode = inner$backend, cpus = inner$ncpus)
      on.exit(parallelMap::parallelStop(), add = TRUE)
      catf("[job(chunk): %s] Using %i CPUs for inner parallelMap parallelization", s, inner$ncpus, con = con)
      p = Sequential$new()
    }
  } else {
    p = Sequential$new()
  }


  measure.memory = (jc$resources$measure.memory %??% FALSE)
  catf("[job(chunk): %s] Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"), con = con)

  # try to pre-fetch some objects from the file system and load registry dependencies
  catf("[job(chunk): %s] Prefetching objects", s, con = con)
  cache = Cache$new(jc$file.dir)
  prefetch(jc, cache)
  loadRegistryDependencies(jc, switch.wd = FALSE)

  runHook(jc, "pre.do.collection", con = con, cache = cache)

  count = 1L
  updates = list()
  next.update = ustamp() + as.integer(runif(1L, 300L, 1800L))
  for (i in seq_len(n.jobs)) {
    job = getJob(jc, jc$defs$job.id[i], cache = cache)
    messages = p$spawn(doJob, job = job, measure.memory = measure.memory)
    if (length(messages) > 0L) {
      updates = c(updates, lapply(messages, "[[", "update"))
      lapply(messages, function(r) catc(r$output, con = con))
    }

    if (length(updates) > 0L && ustamp() > next.update) {
      writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE)
      updates = list()
      count = count + 1L
      next.update = ustamp() + as.integer(runif(1L, 300L, 1800L))
    }
  }

  messages = p$collect()
  if (length(messages) > 0L) {
    updates = c(updates, lapply(messages, "[[", "update"))
    lapply(messages, function(r) catc(r$output, con = con))
  }

  if (length(updates) > 0L) {
    writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE)
  }

  catf("[job(chunk): %s] Calculation finished!", now(), con = con)
  runHook(jc, "post.do.collection", con = con, cache = cache)
  invisible(NULL)
}
