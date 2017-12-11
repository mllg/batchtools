#' @title Find and Filter Jobs
#'
#' @description
#' These functions are used to find and filter jobs, depending on either their parameters (\code{findJobs} and
#' \code{findExperiments}), their tags (\code{findTagged}), or their computational status (all other functions,
#' see \code{\link{getStatus}} for an overview).
#'
#' Note that \code{findQueued}, \code{findRunning}, \code{findOnSystem} and \code{findExpired} are somewhat heuristic
#' and may report misleading results, depending on the state of the system and the \code{\link{ClusterFunctions}} implementation.
#'
#' See \code{\link{JoinTables}} for convenient set operations (unions, intersects, differences) on tables with job ids.
#'
#' @param expr [\code{expression}]\cr
#'   Predicate expression evaluated in the job parameters.
#'   Jobs for which \code{expr} evaluates to \code{TRUE} are returned.
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{\link{data.table}}] with column \dQuote{job.id} containing matched jobs.
#' @seealso \code{\link{getStatus}} \code{\link{JoinTables}}
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
  job.pars = NULL
  setkeyv(mergedJobs(reg, ids, c("job.id", "job.pars"))[vlapply(job.pars, fun), "job.id"], "job.id")
}

#' @export
#' @rdname findJobs
#' @param prob.name [\code{character}]\cr
#'   Exact name of the problem (no substring matching).
#'   If not provided, all problems are matched.
#' @param prob.pattern [\code{character}]\cr
#'   Regular expression pattern to match problem names.
#'   If not provided, all problems are matched.
#' @param algo.name [\code{character}]\cr
#'   Exact name of the problem (no substring matching).
#'   If not provided, all algorithms are matched.
#' @param algo.pattern [\code{character}]\cr
#'   Regular expression pattern to match algorithm names.
#'   If not provided, all algorithms are matched.
#' @param prob.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the problem parameters.
#' @param algo.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the algorithm parameters.
#' @param repls [\code{integer}]\cr
#'   Whitelist of replication numbers. If not provided, all replications are matched.
findExperiments = function(ids = NULL, prob.name = NA_character_, prob.pattern = NA_character_, algo.name = NA_character_, algo.pattern = NA_character_, prob.pars, algo.pars, repls = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, class = "ExperimentRegistry", sync = TRUE)
  assertString(prob.name, na.ok = TRUE, min.chars = 1L)
  assertString(prob.pattern, na.ok = TRUE, min.chars = 1L)
  assertString(algo.name, na.ok = TRUE, min.chars = 1L)
  assertString(algo.pattern, na.ok = TRUE, min.chars = 1L)
  ee = parent.frame()
  tab = mergedJobs(reg, convertIds(reg, ids), c("job.id", "problem", "algorithm", "prob.pars", "algo.pars", "repl"))

  if (!is.na(prob.name)) {
    problem = NULL
    tab = tab[problem == prob.name]
  }

  if (!is.na(prob.pattern)) {
    problem = NULL
    tab = tab[stri_detect_regex(problem, prob.pattern)]
  }

  if (!is.na(algo.name)) {
    algorithm = NULL
    tab = tab[algorithm == algo.name]
  }

  if (!is.na(algo.pattern)) {
    algorithm = NULL
    tab = tab[stri_detect_regex(algorithm, algo.pattern)]
  }

  if (!is.null(repls)) {
    repls = asInteger(repls, any.missing = FALSE)
    repl = NULL
    tab = tab[repl %in% repls]
  }

  if (!missing(prob.pars)) {
    expr = substitute(prob.pars)
    fun = function(pars) eval(expr, pars, enclos = ee)
    prob.pars = NULL
    tab = tab[vlapply(prob.pars, fun)]
  }

  if (!missing(algo.pars)) {
    expr = substitute(algo.pars)
    fun = function(pars) eval(expr, pars, enclos = ee)
    algo.pars = NULL
    tab = tab[vlapply(algo.pars, fun)]
  }

  setkeyv(tab[, "job.id"], "job.id")[]
}


#' @export
#' @rdname findJobs
findSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findSubmitted(reg, convertIds(reg, ids))
}

.findSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  filter(reg$status, ids, c("job.id", "submitted"))[!is.na(submitted), "job.id"]
}


#' @export
#' @rdname findJobs
findNotSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotSubmitted(reg, convertIds(reg, ids))
}

.findNotSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  filter(reg$status, ids, c("job.id", "submitted"))[is.na(submitted), "job.id"]
}


#' @export
#' @rdname findJobs
findStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findStarted(reg, convertIds(reg, ids))
}

.findStarted = function(reg, ids = NULL, batch.ids = getBatchIds(reg, status = "running")) {
  started = batch.id = status = NULL
  bids = batch.ids[status == "running"]$batch.id
  filter(reg$status, ids, c("job.id", "started", "batch.id"))[!is.na(started) | batch.id %in% bids, "job.id"]
}


#' @export
#' @rdname findJobs
findNotStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotStarted(reg, convertIds(reg, ids))
}

.findNotStarted = function(reg, ids = NULL, batch.ids = getBatchIds(reg, status = "running")) {
  started = batch.id = status = NULL
  bids = batch.ids[status == "running"]$batch.id
  filter(reg$status, ids, c("job.id", "started", "batch.id"))[is.na(started) & ! batch.id %chin% bids, "job.id"]
}


#' @export
#' @rdname findJobs
findDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findDone(reg, convertIds(reg, ids))
}

.findDone = function(reg, ids = NULL) {
  done = error = NULL
  filter(reg$status, ids, c("job.id", "done", "error"))[!is.na(done) & is.na(error), "job.id"]
}


#' @export
#' @rdname findJobs
findNotDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findNotDone(reg, convertIds(reg, ids))
}

.findNotDone = function(reg, ids = NULL) {
  done = error = NULL
  filter(reg$status, ids, c("job.id", "done", "error"))[is.na(done) | !is.na(error), "job.id"]
}


#' @export
#' @rdname findJobs
findErrors = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  .findErrors(reg, convertIds(reg, ids))
}

.findErrors = function(reg, ids = NULL) {
  error = NULL
  filter(reg$status, ids, c("job.id", "error"))[!is.na(error), "job.id"]
}


# used in waitForJobs: find jobs which are done or error
.findTerminated = function(reg, ids = NULL) {
  done = NULL
  filter(reg$status, ids, c("job.id", "done"))[!is.na(done), "job.id"]
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
  filter(reg$status, ids, c("job.id", "submitted", "done", "batch.id"))[!is.na(submitted) & is.na(done) & batch.id %chnin% batch.ids$batch.id, "job.id"]
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

  ids[unique(reg$tags[tag %chin% tags, "job.id"], by = "job.id")]
}
