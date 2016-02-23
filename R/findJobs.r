#' @export
#' @rdname findJobs
findSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findSubmitted(reg, ids)
}

.findSubmitted = function(reg, ids = NULL) {
  filter(reg$status, ids)[!is.na(submitted), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotSubmitted(reg, ids)
}

.findNotSubmitted = function(reg, ids = NULL) {
  filter(reg$status, ids)[is.na(submitted), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findStarted(reg, ids)
}

.findStarted = function(reg, ids = NULL) {
  filter(reg$status, ids)[!is.na(started), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotStarted(reg, ids)
}

.findNotStarted = function(reg, ids = NULL) {
  filter(reg$status, ids)[is.na(started), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findDone(reg, ids)
}

.findDone = function(reg, ids = NULL) {
  filter(reg$status, ids)[!is.na(done) & is.na(error), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotDone(reg, ids)
}

.findNotDone = function(reg, ids = NULL) {
  filter(reg$status, ids)[is.na(done) | !is.na(error), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findError = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findError(reg, ids)
}

.findError = function(reg, ids = NULL) {
  filter(reg$status, ids)[!is.na(error), "job.id", with = FALSE]
}


#' @export
#' @param status [\code{character(1)}]\cr
#'  If set to \dQuote{all} (default), \code{findOnSystem} finds all jobs on the system.
#'  If set to \dQuote{queued} or \dQuote{running}, the result is restricted to queued
#'  or running jobs, respectively.
#' @rdname findJobs
findOnSystem = function(ids = NULL, status = "all", reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  assertChoice(status, choices = c("all", "queued", "running"))
  .findOnSystem(reg, ids, status)
}

.findOnSystem = function(reg, ids = NULL, status = "all", batch.ids = getBatchIds(reg, status = status)) {
  if (length(batch.ids) == 0L)
    return(copy(no.ids))
  filter(reg$status, ids)[!is.na(submitted) & is.na(done) & batch.id %in% batch.ids$batch.id, "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findExpired = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findExpired(reg, ids)
}

.findExpired = function(reg, ids = NULL, batch.ids = getBatchIds(reg)) {
  filter(reg$status, ids)[!is.na(submitted) & is.na(done) & batch.id %nin% batch.ids$batch.id, "job.id", with = FALSE]
}


#' @title Find and Filter Jobs
#'
#' @description
#' Use \code{findJobs} to query jobs for which a predicate expression, evaluated on the parameters, yields \code{TRUE}.
#' The other functions can be used to query the computational status.
#' Note that they do not synchronize the registry, thus you are advised to run \code{\link{syncRegistry}} yourself
#' or, if you are really just interested in the status, use \code{\link{getStatus}}.
#' Note that \code{findOnSystem} and \code{findExpired} are somewhat heuristic and may report misleading results, depending on the state of the system.
#'
#' @param expr [\code{expression}]\cr
#'   Predicate expression evaluated in the job parameters.
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{data.table}]. Matching job ids are stored in the column \dQuote{job.id}.
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(identity, i = 1:3, reg = reg)
#' ids = findNotSubmitted(reg = reg)
#'
#' # get all jobs:
#' findJobs(reg = reg)
#'
#' # filter for jobs with parameter i >= 2
#' findJobs(i >= 2, reg = reg)
#'
#' # filter on the computational status
#' findSubmitted(reg = reg)
#' findNotDone(reg = reg)
findJobs = function(expr, ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, strict = TRUE)
  syncRegistry(reg)
  if (missing(expr))
    return(filter(reg$status, ids)[, "job.id", with = FALSE])

  expr = substitute(expr)
  ee = parent.frame()
  fun = function(pars) eval(expr, pars, enclos = ee)
  inner_join(filter(reg$status, ids), reg$defs)[vlapply(pars, fun), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
#' @param prob.name [\code{character}]\cr
#'   Whitelist of problem names. If not provided, all problems are matched.
#' @param algo.name [\code{character}]\cr
#'   Whitelist of algorithm names. If not provided, all algorithms are matched.
#' @param prob.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the problem parameters.
#' @param algo.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the algorithm parameters.
#' @param repls [\code{integer}]\cr
#'   Whitelist of replication numbers. If not provided, all replications are matched.
findExperiments = function(prob.name = NULL, algo.name = NULL, prob.pars, algo.pars, repls = NULL, ids = NULL, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  syncRegistry(reg)

  ee = parent.frame()
  tab = inner_join(filter(reg$status, ids), reg$defs)[, c("job.id", "pars", "problem", "algorithm", "repl"), with = FALSE]

  if (!is.null(prob.name)) {
    assertCharacter(prob.name, any.missing = FALSE)
    tab = tab[problem %in% prob.name]
  }

  if (!is.null(algo.name)) {
    assertCharacter(algo.name, any.missing = FALSE)
    tab = tab[algorithm %in% algo.name]
  }

  if (!missing(prob.pars)) {
    expr = substitute(prob.pars)
    fun = function(pars) eval(expr, pars$prob.pars, enclos = ee)
    tab = tab[vlapply(pars, fun)]
  }

  if (!missing(algo.pars)) {
    expr = substitute(algo.pars)
    fun = function(pars) eval(expr, pars$algo.pars, enclos = ee)
    tab = tab[vlapply(pars, fun)]
  }

  if (!is.null(repls)) {
    repls = asInteger(repls, any.missing = FALSE)
    tab = tab[repl %in% repls]
  }

  return(tab[, "job.id", with = FALSE])
}
