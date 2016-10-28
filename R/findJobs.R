#' @title Find and Filter Jobs
#'
#' @description
#' These functions are used to find and filter jobs, depending on either their parameters (\code{findJobs} and
#' \code{findExperiments}), their tags (\code{findTagged}), or their computational status (all other functions).
#'
#' For a summarizing overview over the status, see \code{\link{getStatus}}.
#' Note that \code{findOnSystem} and \code{findExpired} are somewhat heuristic and may report misleading results,
#' depending on the state of the system and the \code{\link{ClusterFunctions}} implementation.
#'
#' @param expr [\code{expression}]\cr
#'   Predicate expression evaluated in the job parameters.
#'   Jobs for which \code{expr} evaluates to \code{TRUE} are returned.
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{\link{data.table}}] with column \dQuote{job.id} containing matched jobs.
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, i = 1:3, reg = tmp)
#' ids = findNotSubmitted(reg = tmp)
#'
#' # get all jobs:
#' findJobs(reg = tmp)
#'
#' # filter for jobs with parameter i >= 2
#' findJobs(i >= 2, reg = tmp)
#'
#' # filter on the computational status
#' findSubmitted(reg = tmp)
#' findNotDone(reg = tmp)
#'
#' # filter on tags
#' addJobTags(2:3, "my_tag", reg = tmp)
#' findTagged(tags = "my_tag", reg = tmp)
#'
#' # combine filter functions using joins
#' # -> jobs which are not done and not tagged (using an anti-join):
#' ajoin(findNotDone(reg = tmp), findTagged("my_tag", reg = tmp))
findJobs = function(expr, ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  ids = convertIds(reg, ids)
  if (missing(expr))
    return(ids %??% allIds(reg))

  expr = substitute(expr)
  ee = parent.frame()
  fun = function(pars) eval(expr, pars, enclos = ee)
  pars = NULL
  setkeyv(mergedJobs(reg, ids, c("job.id", "pars"))[vlapply(pars, fun), "job.id", with = FALSE], "job.id")[]
}

#' @export
#' @rdname findJobs
#' @param prob.name [\code{character}]\cr
#'   Whitelist of problem names.
#'   If the string starts with \dQuote{~}, it is treated as regular expression.
#'   If not provided, all problems are matched.
#' @param algo.name [\code{character}]\cr
#'   Whitelist of algorithm names.
#'   If the string starts with \dQuote{~}, it is treated as regular expression.
#'   If not provided, all algorithms are matched.
#' @param prob.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the problem parameters.
#' @param algo.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the algorithm parameters.
#' @param repls [\code{integer}]\cr
#'   Whitelist of replication numbers. If not provided, all replications are matched.
findExperiments = function(prob.name = NULL, algo.name = NULL, prob.pars, algo.pars, repls = NULL, ids = NULL, reg = getDefaultRegistry()) {
  strmatch = function(x, pattern) {
    y = split(pattern, ifelse(stri_startswith_fixed(pattern, "~"), "regex", "table"))
    for (p in y$regex)
      y$table = union(y$table, stri_subset_regex(levels(x), stri_sub(p, 2L)))
    x %in% y$table
  }

  assertExperimentRegistry(reg, sync = TRUE)
  ee = parent.frame()
  tab = mergedJobs(reg, convertIds(reg, ids), c("job.id", "pars", "problem", "algorithm", "repl"))

  if (!is.null(prob.name)) {
    assertCharacter(prob.name, any.missing = FALSE, min.chars = 1L)
    problem = NULL
    tab = tab[strmatch(problem, prob.name)]
  }

  if (!is.null(algo.name)) {
    assertCharacter(algo.name, any.missing = FALSE, min.chars = 1L)
    algorithm = NULL
    tab = tab[strmatch(algorithm, algo.name)]
  }

  if (!missing(prob.pars)) {
    expr = substitute(prob.pars)
    fun = function(pars) eval(expr, pars$prob.pars, enclos = ee)
    pars = NULL
    tab = tab[vlapply(pars, fun)]
  }

  if (!missing(algo.pars)) {
    expr = substitute(algo.pars)
    fun = function(pars) eval(expr, pars$algo.pars, enclos = ee)
    pars = NULL
    tab = tab[vlapply(pars, fun)]
  }

  if (!is.null(repls)) {
    repls = asInteger(repls, any.missing = FALSE)
    repl = NULL
    tab = tab[repl %in% repls]
  }

  setkeyv(tab[, "job.id", with = FALSE], "job.id")[]
}


#' @export
#' @rdname findJobs
findSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findSubmitted(reg, convertIds(reg, ids))
}

.findSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  filter(reg$status, ids, c("job.id", "submitted"))[!is.na(submitted), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotSubmitted(reg, convertIds(reg, ids))
}

.findNotSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  filter(reg$status, ids, c("job.id", "submitted"))[is.na(submitted), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findStarted(reg, convertIds(reg, ids))
}

.findStarted = function(reg, ids = NULL) {
  started = NULL
  filter(reg$status, ids, c("job.id", "started"))[!is.na(started), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotStarted(reg, convertIds(reg, ids))
}

.findNotStarted = function(reg, ids = NULL) {
  started = NULL
  filter(reg$status, ids, c("job.id", "started"))[is.na(started), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findDone(reg, convertIds(reg, ids))
}

.findDone = function(reg, ids = NULL) {
  done = error = NULL
  filter(reg$status, ids, c("job.id", "done", "error"))[!is.na(done) & is.na(error), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findNotDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotDone(reg, convertIds(reg, ids))
}

.findNotDone = function(reg, ids = NULL) {
  done = error = NULL
  filter(reg$status, ids, c("job.id", "done", "error"))[is.na(done) | !is.na(error), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findErrors = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findErrors(reg, convertIds(reg, ids))
}

.findErrors = function(reg, ids = NULL) {
  error = NULL
  filter(reg$status, ids, c("job.id", "error"))[!is.na(error), "job.id", with = FALSE]
}


#' @export
#' @rdname findJobs
findOnSystem = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findOnSystem(reg, convertIds(reg, ids))
}

.findOnSystem = function(reg, ids = NULL, status = "all", cols = "job.id", batch.ids = getBatchIds(reg, status = status)) {
  if (length(batch.ids) == 0L)
    return(noIds())
  submitted = done = batch.id = NULL
  filter(reg$status, ids, c("job.id", "submitted", "done", "batch.id"))[!is.na(submitted) & is.na(done) & batch.id %in% batch.ids$batch.id, cols, with = FALSE]
}


#' @export
#' @rdname findJobs
findRunning = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findOnSystem(reg, convertIds(reg, ids), batch.ids = getBatchIds(reg, status = "running"))
}

#' @export
#' @rdname findJobs
findQueued = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findOnSystem(reg, convertIds(reg, ids), batch.ids = getBatchIds(reg, status = "queued"))
}

#' @export
#' @rdname findJobs
findExpired = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findExpired(reg, convertIds(reg, ids))
}

.findExpired = function(reg, ids = NULL, batch.ids = getBatchIds(reg)) {
  submitted = done = batch.id = NULL
  filter(reg$status, ids, c("job.id", "submitted", "done", "batch.id"))[!is.na(submitted) & is.na(done) & batch.id %nin% batch.ids$batch.id, "job.id", with = FALSE]
}

#' @export
#' @rdname findJobs
#' @param tags [\code{character}]\cr
#'   Return jobs which are tagged with any of the tags provided.
findTagged = function(tags = character(0L), ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = allIds(reg))
  assertCharacter(tags, any.missing = FALSE, pattern = "^[[:alnum:]_.]+$", min.len = 1L)
  tag = NULL

  ids[unique(reg$tags[tag %in% tags, "job.id", with = FALSE], by = "job.id")]
}
