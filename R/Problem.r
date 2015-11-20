#' @title Define Problems and Algorithms
#'
#' @description
#' Problems may consist of up to two parts. A static, immutable part (\code{data} in \code{addProblem})
#' and a dynamic, stochastic part (\code{fun} in \code{addProblem}).
#' For example, for statistical learning problems a data frame would be the static problem part while
#' a resampling function would be the stochastic part which creates problem instance.
#' This instance is then typically passed to a learning algorithm like a wrapper around a statistical model
#' (\code{fun} in \code{addAlgorithm}).
#'
#' The functions serialize the components to the file system and register the respective problem or algorithm
#' names in the \code{\link{ExperimentRegistry}}.
#'
#' @param name [\code{character(1)}]\cr
#'   Unique identifier for the problem or algorithm.
#' @param data [\code{ANY}]\cr
#'   Static problem part. Default is \code{NULL}.
#' @param fun [\code{function}]\cr
#'   For \code{addProblem}, the stochastic problem part. The static part is passed to this function with name
#'   \dQuote{data}, a reduced \code{\link{JobCollection}} with name \dQuote{job}.
#'   For \code{addAlgorithm}, the algorithm function. The static part is passed as \dQuote{data}, the generated
#'   problem instance is passed as \dQuote{problem} and a \code{\link{JobCollection}} as \dQuote{job}.
#'   Use \code{...} if you want to ignore some of these arguments.
#' @param seed [\code{integer(1)}]\cr
#'   Start seed for this problem. This allows the \dQuote{synchronization} of a stochastic
#'   problem across algorithms, so that different algorithms are evaluated on the same stochastic instance.
#'   If the problem seed is defined, the seeding mechanism works as follows:
#'   (1) Before the dynamic part of a problem is instantiated,
#'   the seed of the problem + replication number is set, so for the first
#'   replication the exact problem seed is used. (2) The stochastic part of the problem is
#'   instantiated. (3) From now on the usual experiment seed of the registry is used,
#'   see \code{\link{ExperimentRegistry}}.
#'   If \code{seed} is set to \code{NULL} (default) this extra problem seeding is switched off, meaning
#'   different algorithms see different stochastic versions of the same problem.
#' @template expreg
#' @return [\code{Problem}]. Object of class \dQuote{Problem} (invisibly).
#' @name ProblemAlgorithm
#' @rdname ProblemAlgorithm
#' @aliases Problem Algorithm
#' @export
addProblem = function(name, data = NULL, fun = NULL, seed = NULL, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, ...) data
  } else {
    assertFunction(fun, args = c("job", "data"))
  }
  if (!is.null(seed))
    seed = asCount(seed, positive = TRUE)

  prob = setClasses(list(name = name, seed = seed, data = data, fun = fun), "Problem")
  writeRDS(prob, file = file.path(reg$file.dir, "problems", sprintf("%s.rds", name)))
  reg$problems = union(reg$problems, name)
  saveRegistry(reg)
  invisible(prob)
}

#' @export
#' @rdname ProblemAlgorithm
removeProblem = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name)
  assertSubset(name, reg$problems)
  pars = NULL

  fns = file.path(reg$file.dir, "problems", sprintf("%s.rds", name))
  def.ids = reg$defs[vcapply(pars, "[[", "prob.name") == name, "def.id", with = FALSE]
  job.ids = reg$status[def.ids, "job.id", on = "def.id", nomatch = 0L, with = FALSE]

  info("Removing Problem '%s' and %i corresponding jobs ...", name, nrow(job.ids))
  file.remove(fns)
  reg$problems = setdiff(reg$problems, name)
  reg$defs = reg$defs[!def.ids]
  reg$status = reg$status[!job.ids]
  saveRegistry(reg)
  invisible(TRUE)
}
