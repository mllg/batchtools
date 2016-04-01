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
#' @return [\code{data.table}]. Data table with updates on the computational
#' status, i.e. a table with the columns \dQuote{job.id}, \dQuote{started}
#' (unix time stamp of job start), \dQuote{done} (unix time stamp of job
#' termination), \dQuote{error} (error message as string) and \code{memory}
#' (memory usage as double).
#' @export
doJobCollection = function(jc, con = stdout()) {
  UseMethod("doJobCollection")
}


#' @export
doJobCollection.character = function(jc, con = stdout()) {
  doJobCollection(readRDS(jc), con = con)
}


#' @export
doJobCollection.JobCollection = function(jc, con = stdout()) {
  if (!inherits(con, "connection")) {
    con = file(con, open = "wt")
    on.exit(close(con))
  }
  n.jobs = nrow(jc$defs)
  ncpus = min(n.jobs, jc$resources$chunk.ncpus %??% 1L)
  measure.memory = ncpus == 1L && (jc$resources$measure.memory %??% FALSE)
  cache = Cache$new(jc$file.dir)

  s = stamp()
  catf("[job(chunk): %s] Starting calculation of %i jobs", s, n.jobs, con = con)
  catf("[job(chunk): %s] Setting working directory to '%s'", s, jc$work.dir, con = con)
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd), add = TRUE)

  loadRegistryDependencies(jc, switch.wd = FALSE)

  catf("[job(chunk): %s] Using %i cpus", s, ncpus, con = con)
  catf("[job(chunk): %s] Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"), con = con)
  catf("[job(chunk): %s] Prefetching objects", s, ifelse(measure.memory, "enabled", "disabled"), con = con)
  prefetch(jc, cache)

  count = 1L
  updates = list()
  next.update = now() + as.integer(runif(1L, 300L, 1800L))
  if (ncpus == 1L) {
    p = Sequential$new()
  } else {
    if (testOS("windows"))
      p = Snow$new(ncpus)
    else
      p = Parallel$new(ncpus)
  }

  for (i in seq_len(n.jobs)) {
    job = getJob(jc, jc$defs$job.id[i], cache = cache)
    results = p$spawn(doJob, job = job, measure.memory = measure.memory)
    if (length(results) > 0L) {
      updates = c(updates, lapply(results, "[[", "update"))
      lapply(results, function(r) catf(r$output, con = con))
    }

    if (length(updates) > 0L && now() > next.update) {
      writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE)
      updates = list()
      count = count + 1L
      next.update = now() + as.integer(runif(1L, 300L, 1800L))
    }
  }

  results = p$collect()
  if (length(results) > 0L) {
    updates = c(updates, lapply(results, "[[", "update"))
    lapply(results, function(r) catf(r$output, con = con))
  }
  writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE)

  catf("[job(chunk): %s] Calculation finished!", stamp(), con = con)
  invisible(TRUE)
}
