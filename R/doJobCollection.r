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
#' @return [\code{data.table}]. Data table with updates on the computational
#' status, i.e. a table with the columns \dQuote{job.id}, \dQuote{started}
#' (unix time stamp of job start), \dQuote{done} (unix time stamp of job
#' termination), \dQuote{error} (error message as string) and \code{memory}
#' (memory usage as double).
#' @export
doJobCollection = function(jc) {
  UseMethod("doJobCollection")
}


#' @export
doJobCollection.character = function(jc) {
  doJobCollection(readRDS(jc))
}


#' @export
doJobCollection.JobCollection = function(jc) {
  n.jobs = nrow(jc$defs)
  ncpus = min(n.jobs, jc$resources$chunk.ncpus %??% 1L)
  measure.memory = ncpus == 1L && (jc$resources$measure.memory %??% FALSE)
  cache = Cache$new(jc$file.dir)

  s = stamp()
  catf("[job(chunk): %s] Starting calculation of %i jobs", s, n.jobs)
  catf("[job(chunk): %s] Setting working directory to '%s'", s, jc$work.dir)
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd))

  loadRegistryDependencies(jc, switch.wd = FALSE)

  catf("[job(chunk): %s] Using %i cpus", s, ncpus)
  catf("[job(chunk): %s] Memory measurement %s", s, ifelse(measure.memory, "enabled", "disabled"))
  catf("[job(chunk): %s] Prefetching objects", s, ifelse(measure.memory, "enabled", "disabled"))
  prefetch(jc, cache)

  count = 1L
  updates = list()
  next.update = now() + as.integer(runif(1L, 300L, 1800L))
  p = if (ncpus == 1L || testOS("windows")) Sequential$new() else Parallel$new(ncpus)

  for (i in seq_len(n.jobs)) {
    results = p$spawn(doJob(jc$defs$job.id[i], jc, cache, measure.memory = measure.memory))
    if (length(results) > 0L) {
      updates = c(updates, lapply(results, "[[", "update"))
      lapply(results, function(r) catf(r$output))
    }

    if (length(updates) > 0L && now() > next.update) {
      writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count)), wait = TRUE, compress = jc$compress)
      updates = list()
      count = count + 1L
      next.update = now() + as.integer(runif(1L, 300L, 1800L))
    }
  }

  results = p$collect()
  if (length(results) > 0L) {
    updates = c(updates, lapply(results, "[[", "update"))
    lapply(results, function(r) catf(r$output))
  }
  writeRDS(rbindlist(updates), file = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, count + 1L)), wait = TRUE, compress = jc$compress)

  catf("[job(chunk): %s] Calculation finished!", stamp())
  invisible(TRUE)
}
