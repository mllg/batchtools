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
#' @param con [\code{\link[base]{connection}}]\cr
#'   A connection to redirect the output to.
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
doJobCollection.JobCollection = function(jc, con = stdout()) {
  capture = function(expr) {
    output = character(0L)
    con = textConnection("output","w", local = TRUE)
    sink(file = con)
    sink(file = con, type = "message")
    on.exit({ sink(type = "message"); sink(); close(con) })
    res = try(eval(expr, parent.frame()))
    list(output = output, res = res)
  }

  doJob = function(id, write.update = FALSE, measure.memory = FALSE) {
    catf("[job(%i): %s] Starting job with job.id=%i", id, stamp(), id, con = con)

    update = list(job.id = id, started = now(), done = NA_integer_, error = NA_character_, memory = NA_real_)
    if (measure.memory) {
      gc(reset = TRUE)
      result = capture(execJob(getJob(jc, id, cache)))
      update$memory = sum(gc()[, 6L])
    } else {
      result = capture(execJob(getJob(jc, id, cache)))
    }
    update$done = now()


    if (length(result$output) > 0L)
      catf("[job(%i): %s] %s", id, stamp(), result$output, con = con)

    if (is.error(result$res)) {
      catf("[job(%i): %s] Job terminated with an exception", id, stamp(), con = con)
      update$error = stri_trim_both(as.character(result$res))
    } else {
      catf("[job(%i): %s] Job terminated successfully", id, stamp(), con = con)
      writeRDS(result$res, file = file.path(jc$file.dir, "results", sprintf("%i.rds", id)), compress = jc$compress)
    }

    if (write.update) {
      fn = file.path(jc$file.dir, "updates", sprintf("%s-%i.rds", jc$job.hash, id, 1L))
      writeRDS(update, file = fn, wait = TRUE, compress = jc$compress)
    }

    return(update)
  }

  loadRegistryPackages(jc$packages, jc$namespaces)
  stamp = function() strftime(Sys.time())
  n.jobs = nrow(jc$defs)

  catf("[job(chunk): %s] Starting calculation of %i jobs", stamp(), n.jobs, con = con)

  catf("[job(chunk): %s] Setting working directory to '%s'", stamp(), jc$work.dir, con = con)
  prev.wd = getwd()
  setwd(jc$work.dir)
  on.exit(setwd(prev.wd))

  cache = Cache(jc$file.dir)
  ncpus = jc$resources$ncpus %??% 1L
  measure.memory = jc$resources$measure.memory %??% FALSE
  if (measure.memory)
    catf("[job(chunk): %s] Enabling memory measurement", stamp(), con = con)

  if (n.jobs > 1L && ncpus > 1L) {
    prefetch(jc, cache)
    parallel::mclapply(jc$defs$job.id, doJob, mc.cores = ncpus, mc.preschedule = FALSE, write.update = TRUE, measure.memory = measure.memory)
  } else {
    updates = vector("list", n.jobs)
    update.interval = 1800L
    last.update = now()
    fn = file.path(jc$file.dir, "updates", sprintf("%s.rds", jc$job.hash))
    for (i in seq_len(n.jobs)) {
      updates[[i]] = as.data.table(doJob(jc$defs$job.id[i], write.update = FALSE, measure.memory = measure.memory))
      if (now() - last.update > update.interval) {
        writeRDS(rbindlist(updates), file = fn, wait = TRUE, compress = jc$compress)
        last.update = now()
        write.update = FALSE
      } else {
        write.update = TRUE
      }
    }
    if (write.update)
      writeRDS(rbindlist(updates), file = fn, wait = TRUE, compress = jc$compress)
  }

  catf("[job(chunk): %s] Calculation finished ...", stamp(), con = con)
  invisible(TRUE)
}

#' @export
doJobCollection.character = function(jc, con = stdout()) {
  doJobCollection(readRDS(jc), con = con)
}
