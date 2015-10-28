#' @title Execute jobs
#'
#' @param jd [\code{\link{JobDescription}}]\cr
#'   Either an object of class \dQuote{JobDescription} as returned by \code{\link{makeJobDescription}} or a
#'   string point to file containing a \dQuote{JobDescription} (saved with \code{\link[base]{saveRDS}}).
#' @param con [\code{\link[base]{connection}}]\cr
#'   A connection to redirect the output to.
#' @return [\code{data.table}]. Data table with updates on the computational status, i.e.
#'   a table with the columns \dQuote{job.id}, \dQuote{started} (unix time stamp of job start),
#'   \dQuote{done} (unix time stamp of job termination), \dQuote{error} (error message as string)
#'   and \code{memory} (memory usage as double).
#' @export
doJobs = function(jd, con = stdout()) {
  UseMethod("doJobs")
}

#' @export
doJobs.JobDescription = function(jd, con = stdout()) {
  capture = function(expr) {
    output = character(0L)
    con = textConnection("output","w", local = TRUE)
    sink(file = con)
    sink(file = con, type = "message")
    on.exit({ sink(type = "message"); sink(); close(con) })
    res = try(eval(expr, parent.frame()))
    list(output = output, res = res)
  }

  doJob = function(id, write.update = FALSE) {
    update = list(job.id = id, started = NA_integer_, done = NA_integer_, error = NA_character_)

    catf("[job(%i): %s] Starting job with job.id=%i", id, stamp(), id, con = con)

    update$started = now()
    result = capture(execJob(jd, id, cache))
    update$done = now()

    if (length(result$output) > 0L)
      catf("[job(%i): %s] %s", id, stamp(), result$output, con = con)

    if (is.error(result$res)) {
      catf("[job(%i): %s] Job terminated with an exception", id, stamp(), con = con)
      update$error = stri_trim_both(as.character(result$res))
    } else {
      catf("[job(%i): %s] Job terminated successfully", id, stamp(), con = con)
      write(result$res, file = file.path(jd$file.dir, "results", sprintf("%i.rds", id)))
    }

    if (write.update) {
      fn = file.path(jd$file.dir, "updates", sprintf("%s-%i.rds", jd$job.hash, id, 1L))
      write(update, file = fn, wait = TRUE)
    }

    return(update)
  }

  loadRegistryPackages(jd$packages, jd$namespaces)
  stamp = function() strftime(Sys.time())
  n.jobs = nrow(jd$defs)

  catf("[job(chunk): %s] Starting calculation of %i jobs", stamp(), n.jobs, con = con)

  catf("[job(chunk): %s] Setting working directory to '%s'", stamp(), jd$work.dir, con = con)
  prev.wd = getwd()
  setwd(jd$work.dir)
  on.exit(setwd(prev.wd))

  cache = Cache(jd$file.dir)
  ncpus = jd$resources$ncpus %??% 1L

  if (n.jobs > 1L && ncpus > 1L) {
    prefetch(jd, cache)
    parallel::mclapply(jd$defs$job.id, doJob, mc.cores = ncpus, mc.preschedule = FALSE, write.update = TRUE)
  } else {
    updates = vector("list", n.jobs)
    update.interval = 1800L
    last.update = now()
    fn = file.path(jd$file.dir, "updates", sprintf("%s.rds", jd$job.hash))
    for (i in seq_len(n.jobs)) {
      updates[[i]] = as.data.table(doJob(jd$defs$job.id[i], write.update = FALSE))
      if (now() - last.update > update.interval) {
        write(rbindlist(updates), file = fn, wait = TRUE)
        last.update = now()
        write.update = FALSE
      } else {
        write.update = TRUE
      }
    }
    if (write.update)
      write(rbindlist(updates), file = fn, wait = TRUE)
  }

  catf("[job(chunk): %s] Calculation finished ...", stamp(), con = con)
  invisible(TRUE)
}

#' @export
doJobs.character = function(jd, con = stdout()) {
  doJobs(readRDS(jd), con = con)
}
