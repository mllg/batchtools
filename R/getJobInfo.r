#' @title Query Job Information
#'
#' @description
#' \code{getJobStatus} returns the internal table which stores information about the computational
#' status of jobs, \code{getJobResources} a table with the resources which were set to submit the jobs
#' and \code{getJobPars} a table with the job parameters.
#'
#' \code{getJobInfo} returns all these tables joined.
#'
#' @templateVar ids.default all
#' @template ids
#' @param pars.as.cols [\code{logical(1)}]\cr
#'   Transform the job parameters to data frame columns? Default is \code{FALSE}.
#' @param prefix.pars [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the prefix \dQuote{par.} is used for a regular
#'   \code{\link{Registry}} to prefix parameter names of jobs while the
#'   prefixes \dQuote{prob.par.} and \dQuote{algo.par.} are used for an
#'   \code{\link{ExperimentRegistry}}. Has no effect if \code{pars.as.cols} is
#'   \code{FALSE}.
#' @param resources.as.cols [\code{logical(1)}]\cr
#'   Transform the resources data frame columns? Default is \code{FALSE}.
#' @param reg [\code{\link{Registry}}]\cr
#'   Registry.
#' @return [\code{data.frame}] with the following columns in no particular order:
#' \describe{
#'   \item{job.id}{Unique Job ID as integer.}
#'   \item{submitted}{Time the job was submitted to the batch system as \code{\link[base]{POSIXct}}.}
#'   \item{started}{Time the job was started on the batch system as \code{\link[base]{POSIXct}}.}
#'   \item{done}{Time the job terminated (successfully or with an error) as \code{\link[base]{POSIXct}}.}
#'   \item{error}{Either \code{NA} if the job terminated successfully or the error message.}
#'   \item{time.queued}{Time in seconds (as \code{\link[base]{difftime}}) the job was queued.}
#'   \item{time.running}{Time in seconds (as \code{\link[base]{difftime}}) the job was running.}
#'   \item{memory}{Estimate of the memory usage.}
#'   \item{batch.id}{Batch ID as reported by the scheduler.}
#'   \item{job.hash}{Unique string identifying the job or chunk.}
#'   \item{resources}{List of computational resources set for this job.}
#'   \item{resources.hash}{MD5 hash of the resource list.}
#'   \item{pars}{List of parameters/arguments for this job.}
#'   \item{pars.hash}{MD5 hash of the job parameters.}
#'   \item{problem}{Only for \code{\link{ExperimentRegistry}}: the problem identifier.}
#'   \item{algorithm}{Only for \code{\link{ExperimentRegistry}}: the algorithm identifier.}
#' }
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' f = function(x) if (x < 0) stop("x must be > 0") else sqrt(x)
#' batchMap(f, x = c(-1, 0, 1), reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#'
#' getJobInfo(reg = reg, pars.as.cols = TRUE)
getJobInfo = function(ids = NULL, pars.as.cols = FALSE, prefix.pars = FALSE, resources.as.cols = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = asIds(reg, ids, default = .findAll(reg))

  getJobStatus(ids, reg = reg)[getJobResources(ids, reg = reg)][getJobDefs(ids, pars.as.cols = pars.as.cols, prefix.pars = prefix.pars, reg = reg)]
}

#' @export
#' @rdname getJobInfo
getJobStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  ids = asIds(reg, ids, default = .findAll(reg))
  tab = reg$status[ids]
  tab[, "submitted" := as.POSIXct(get("submitted"), origin = "1970-01-01")]
  tab[, "started" := as.POSIXct(get("started"), origin = "1970-01-01")]
  tab[, "done" := as.POSIXct(get("done"), origin = "1970-01-01")]
  tab[, "time.queued" := as.difftime(get("started") - get("submitted"), units = "secs")]
  tab[, "time.running" := as.difftime(get("done") - get("started"), units = "secs")]
  tab[, !c("def.id", "resource.id"), with = FALSE]
}

getJobDefs = function(ids = NULL, pars.as.cols = FALSE, prefix.pars = FALSE, reg = getDefaultRegistry()) {
  ids = asIds(reg, ids, default = .findAll(reg))
  tab = reg$status[ids][reg$defs, c("job.id", names(reg$defs)), on = "def.id", nomatch = 0L, with = FALSE]
  parsAsCols(tab, pars.as.cols, prefix.pars, reg = reg)
  tab[, !"def.id", with = FALSE]
}


#' @export
#' @rdname getJobInfo
getJobResources = function(ids = NULL, resources.as.cols = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = asIds(reg, ids, default = .findAll(reg))
  assertFlag(resources.as.cols)

  tab = merge(reg$status[ids, c("job.id", "resource.id"), with = FALSE], reg$resources, by = "resource.id", all.x = TRUE)
  if (resources.as.cols) {
    new.cols = rbindlist(tab$resources, fill = TRUE)
    if (nrow(new.cols) > 0L)
      tab[, names(new.cols) := new.cols]
    tab[, "resources" := NULL]
  }
  setkeyv(tab, "job.id")
  tab[, !"resource.id", with = FALSE]
}

#' @export
#' @rdname getJobInfo
getJobPars = function(ids = NULL, pars.as.cols = FALSE, prefix.pars = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(prefix.pars)
  ids = asIds(reg, ids, default = .findAll(reg))
  def.cols = c("job.id", setdiff(names(reg$defs), c("def.id", "pars.hash")))

  tab = reg$status[ids][reg$defs, def.cols, on = "def.id", nomatch = 0L, with = FALSE]
  parsAsCols(tab, TRUE, prefix.pars, reg = reg)
  tab[]
}

parsAsCols = function(tab, pars.as.cols, prefix.pars, reg = getDefaultRegistry()) {
  UseMethod("parsAsCols", object = reg)
}

parsAsCols.Registry = function(tab, pars.as.cols, prefix.pars, reg = getDefaultRegistry()) {
  if (pars.as.cols) {
    new.cols = rbindlist(tab$pars)
    if (nrow(new.cols) >= 1L) {
      if (prefix.pars)
        setnames(new.cols, names(new.cols), paste0("par.", names(new.cols)))
      tab[, names(new.cols) := new.cols]
    }
    tab[, "pars" := NULL, with = FALSE]
  }
}

parsAsCols.ExperimentRegistry = function(tab, pars.as.cols, prefix.pars, reg = getDefaultRegistry()) {
  if (pars.as.cols) {
    new.cols = rbindlist(lapply(tab$pars, unlist, recursive = FALSE), fill = TRUE)
    pattern = "^(prob|algo).pars."
    replacement = if (prefix.pars) "$1.par." else ""
    setnames(new.cols, names(new.cols), stri_replace_all_regex(names(new.cols), pattern, replacement))
    tab[, names(new.cols) := new.cols]
    tab[, "pars" := NULL]
  }
}
