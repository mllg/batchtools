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
  loadRegistryPackages(jd$packages, jd$namespaces)
  stamp = function() strftime(Sys.time())
  njobs = nrow(jd$defs)

  catf("[job(chunk): %s] Starting calculation of %i jobs", stamp(), njobs, con = con)

  catf("[job(chunk): %s] Setting working directory to '%s'", stamp(), jd$work.dir, con = con)
  prev.wd = getwd()
  setwd(jd$work.dir)
  on.exit(setwd(prev.wd))
  cache = Cache(jd$file.dir, enable = getOption("batchtools.ncpus", 1L) == 1L)

  updates = parallel::mclapply(seq_len(njobs), function(i) {
    jid = jd$defs$job.id[i]
    updates = list(job.id = jid, started = NA_integer_, done = NA_integer_, error = NA_character_)

    catf("[job(%i): %s] Starting job with job.id=%i", jid, stamp(), jid, con = con)
    updates$started = now()

    result = capture(execJob(jd, i, cache))

    updates$done = now()
    if (length(result$output) > 0L)
      catf("[job(%i): %s] %s", jid, stamp(), result$output, con = con)

    if (is.error(result$res)) {
      catf("[job(%i): %s] Job %i/%i terminated with an exception", jid, stamp(), i, njobs, con = con)
      updates$error = stri_trim_both(as.character(result$res))
    } else {
      catf("[job(%i): %s] Job %i/%i terminated successfully", jid, stamp(), i, njobs, con = con)
      write(result$res, file = file.path(jd$file.dir, "results", sprintf("%i.rds", jid)))
    }
    updates
  }, mc.cores = getOption("batchtools.ncpus", 1L))
  updates = rbindlist(updates)

  # save update file
  catf("[job(chunk): %s] Calculation finished, saving database files ...", stamp(), con = con)
  fn = file.path(jd$file.dir, "updates", sprintf("%s.rds", jd$job.hash))
  write(updates, file = fn, wait = TRUE)
  invisible(updates)
}

#' @export
doJobs.character = function(jd, con = stdout()) {
  doJobs(readRDS(jd), con = con)
}

Cache = function(file.dir, enable = TRUE) {
  force(file.dir)
  cache = new.env(parent = emptyenv())
  force(cache)

  if (enable) {
    function(id, uri = id) {
      if (is.null(cache[[id]]) || cache[[id]]$uri != uri) {
        fn = file.path(file.dir, sprintf("%s.rds", uri))
        cache[[id]] = list(uri = uri, obj = if (file.exists(fn)) readRDS(fn) else NULL)
      }
      return(cache[[id]]$obj)
    }
  } else {
    function(id, uri) {
      fn = file.path(file.dir, sprintf("%s.rds", uri))
      if (file.exists(fn)) readRDS(fn) else NULL
    }
  }
}
