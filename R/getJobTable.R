#' @title Query Job Information
#'
#' @description
#' \code{getJobStatus} returns the internal table which stores information about the computational
#' status of jobs, \code{getJobResources} a table with the resources which were set to submit the jobs
#' and \code{getJobPars} a table with the job parameters.
#'
#' \code{getJobTable} returns all these tables joined.
#'
#' @templateVar ids.default all
#' @template ids
#' @param pars.as.cols [\code{logical(1)}]\cr
#'   Transform the job parameters to data frame columns? Defaults to \code{TRUE} if all parameters
#'   are atomic vectors, \code{FALSE} otherwise.
#' @param resources.as.cols [\code{logical(1)}]\cr
#'   Transform the resources data frame columns? Default is \code{FALSE}.
#' @param prefix [\code{logical(1)}]\cr
#'   If set to \code{TRUE} (default), the prefix \dQuote{par.} is used to name column names of parameters
#'   for a \code{\link{Registry}} and prefixes \dQuote{prob.par.} and \dQuote{algo.par.} are used to name
#'   the columns of a \code{\link{ExperimentRegistry}}. Resources are prefixed with \dQuote{res.}.
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
#'   \item{pars}{List of parameters/arguments for this job.}
#'   \item{pars.hash}{MD5 hash of the job parameters.}
#'   \item{problem}{Only for \code{\link{ExperimentRegistry}}: the problem identifier.}
#'   \item{algorithm}{Only for \code{\link{ExperimentRegistry}}: the algorithm identifier.}
#' }
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(x) if (x < 0) stop("x must be > 0") else sqrt(x)
#' batchMap(f, x = c(-1, 0, 1), reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#'
#' getJobTable(reg = reg)
getJobTable = function(ids = NULL, pars.as.cols = NULL, resources.as.cols = FALSE, prefix = FALSE, reg = getDefaultRegistry()) {
  inner_join(inner_join(getJobStatus(ids, reg = reg), getJobDefs(ids, pars.as.cols = pars.as.cols, prefix = prefix, reg = reg)),
    getJobResources(ids = ids, resources.as.cols = resources.as.cols, reg = reg))
}

#' @export
#' @rdname getJobTable
getJobStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)

  tab = filter(reg$status, ids)[, !c("def.id", "resource.id"), with = FALSE]
  tab[, "submitted" := as.POSIXct(submitted, origin = "1970-01-01")]
  tab[, "started" := as.POSIXct(started, origin = "1970-01-01")]
  tab[, "done" := as.POSIXct(done, origin = "1970-01-01")]
  tab[, "time.queued" := as.difftime(started - submitted, units = "secs")]
  tab[, "time.running" := as.difftime(done - started, units = "secs")]
  tab[]
}

#' @export
#' @rdname getJobTable
getJobDefs = function(ids = NULL, pars.as.cols = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  if (!is.null(pars.as.cols))
    assertFlag(pars.as.cols)
  assertFlag(prefix)
  tab = inner_join(filter(reg$status, ids), reg$defs)[, c("job.id", names(reg$defs)), with = FALSE]
  parsAsCols(tab, pars.as.cols, prefix, reg = reg)
  tab[, !"def.id", with = FALSE]
}

#' @export
#' @rdname getJobTable
getJobResources = function(ids = NULL, resources.as.cols = FALSE, prefix = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(prefix)

  tab = merge(filter(reg$status, ids), reg$resources, all.x = TRUE, by = "resource.id")[, c("job.id", names(reg$resources)), with = FALSE]
  if (resources.as.cols) {
    new.cols = rbindlist(tab$resources, fill = TRUE)
    if (nrow(new.cols) > 0L)
      tab[, names(new.cols) := new.cols]
    tab[, "resources" := NULL]
  }
  setkeyv(tab, "job.id")
  tab[, !c("resource.id", "resource.hash"), with = FALSE]
}

#' @export
#' @rdname getJobTable
getJobPars = function(ids = NULL, pars.as.cols = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(prefix)
  def.cols = c("job.id", setdiff(names(reg$defs), c("def.id", "pars.hash")))
  tab = inner_join(filter(reg$status, ids), reg$defs)[, def.cols, with = FALSE]
  parsAsCols(tab, pars.as.cols, prefix, reg = reg)
  tab[]
}

parsAsCols = function(tab, pars.as.cols, prefix, reg = getDefaultRegistry()) {
  UseMethod("parsAsCols", object = reg)
}

parsAsCols.Registry = function(tab, pars.as.cols, prefix, reg = getDefaultRegistry()) {
  if (pars.as.cols %??% qtestr(tab$pars[-2L], c("v", "L"), depth = 2L)) {
    new.cols = rbindlist(tab$pars)
    if (ncol(new.cols) > 0L) {
      if (prefix)
        setnames(new.cols, names(new.cols), stri_join("par.", names(new.cols)))
      tab[, names(new.cols) := new.cols]
    }
    tab[, "pars" := NULL, with = FALSE]
  }
}

parsAsCols.ExperimentRegistry = function(tab, pars.as.cols, prefix, reg = getDefaultRegistry()) {
  if (pars.as.cols %??% qtestr(tab$pars, c("v", "L"), depth = 2L)) {
    new.cols = rbindlist(lapply(tab$pars, unlist, recursive = FALSE), fill = TRUE)
    if (ncol(new.cols) > 0L) {
      pattern = "^(prob|algo).pars."
      replacement = if (prefix) "$1.par." else ""
      setnames(new.cols, names(new.cols), stri_replace_all_regex(names(new.cols), pattern, replacement))
      tab[, names(new.cols) := new.cols]
    }
    tab[, "pars" := NULL]
  }
}
