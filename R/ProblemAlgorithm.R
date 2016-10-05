#' @title Define Problems and Algorithms for Experiments
#'
#' @description
#' Problems may consist of up to two parts: A static, immutable part (\code{data} in \code{addProblem})
#' and a dynamic, stochastic part (\code{fun} in \code{addProblem}).
#' For example, for statistical learning problems a data frame would be the static problem part while
#' a resampling function would be the stochastic part which creates problem instance.
#' This instance is then typically passed to a learning algorithm like a wrapper around a statistical model
#' (\code{fun} in \code{addAlgorithm}).
#'
#' The functions serialize all components to the file system and register the respective problem or algorithm
#' names in the \code{\link{ExperimentRegistry}}.
#'
#' \code{getProblemIds} and \code{getAlgorithmIds} can be used to retrieve the IDs of already defined problems
#' and algorithms, respectively.
#'
#' @param name [\code{character(1)}]\cr
#'   Unique identifier for the problem or algorithm.
#' @param data [\code{ANY}]\cr
#'   Static problem part. Default is \code{NULL}.
#' @param fun [\code{function}]\cr
#'   For \code{addProblem}, the function defining the stochastic problem part.
#'   The static part is passed to this function with name \dQuote{data} and the \code{\link{Job}}/\code{\link{Experiment}}
#'   is passed as \dQuote{job}.
#'   Therefore, your function must have the formal arguments \dQuote{job} and \dQuote{data} (or dots \code{...}).
#'
#'   For \code{addAlgorithm}, the algorithm function. The static part is passed as \dQuote{data}, the generated
#'   problem instance is passed as \dQuote{instance} and the \code{\link{Job}}/\code{\link{Experiment}} as \dQuote{job}.
#'   Therefore, your function must have the formal arguments \dQuote{job}, \dQuote{data} and \dQuote{instance} (or dots \code{...}).
#'
#'   If you do not provide a function, it defaults to a function which just returns the data part (Problem) or the instance (Algorithm).
#' @param seed [\code{integer(1)}]\cr
#'   Start seed for this problem. This allows the \dQuote{synchronization} of a stochastic
#'   problem across algorithms, so that different algorithms are evaluated on the same stochastic instance.
#'   If the problem seed is defined, the seeding mechanism works as follows:
#'   (1) Before the dynamic part of a problem is instantiated,
#'   the seed of the problem + [replication number] - 1 is set, i.e. the first
#'   replication uses the problem seed. (2) The stochastic part of the problem is
#'   instantiated. (3) From now on the usual experiment seed of the registry is used,
#'   see \code{\link{ExperimentRegistry}}.
#'   If \code{seed} is set to \code{NULL} (default), the job seed is used to instantiate the problem and
#'   different algorithms see different stochastic instances of the same problem.
#' @template expreg
#' @return [\code{Problem}]. Object of class \dQuote{Problem} (invisibly).
#' @name ProblemAlgorithm
#' @rdname ProblemAlgorithm
#' @aliases Problem Algorithm
#' @export
#' @examples
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#' addProblem("p1", fun = function(job, data) data, reg = tmp)
#' addProblem("p2", fun = function(job, data) job, reg = tmp)
#' getProblemIds(reg = tmp)
#'
#' addAlgorithm("a1", fun = function(job, data, instance) instance, reg = tmp)
#' getAlgorithmIds(reg = tmp)
#'
#' removeAlgorithms("a1", reg = tmp)
#' getAlgorithmIds(reg = tmp)
addProblem = function(name, data = NULL, fun = NULL, seed = NULL, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name, min.chars = 1L)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, ...) data
  } else {
    assert(checkFunction(fun, args = c("job", "data")), checkFunction(fun, args = "..."))
  }
  if (!is.null(seed))
    seed = asCount(seed, positive = TRUE)

  prob = setClasses(list(name = name, seed = seed, data = data, fun = fun), "Problem")
  writeRDS(prob, file = getProblemURI(reg, name))
  reg$defs$problem = addlevel(reg$defs$problem, name)
  saveRegistry(reg)
  invisible(prob)
}

#' @export
#' @rdname ProblemAlgorithm
removeProblems = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE, running.ok = FALSE)
  assertCharacter(name, any.missing = FALSE)
  assertSubset(name, levels(reg$defs$problem))

  problem = NULL
  for (nn in name) {
    def.ids = reg$defs[problem == nn, "def.id", with = FALSE]
    job.ids = filter(def.ids, reg$status, "job.id")

    info("Removing Problem '%s' and %i corresponding jobs ...", nn, nrow(job.ids))
    file.remove(getProblemURI(reg, nn))
    reg$defs = reg$defs[!def.ids]
    reg$status = reg$status[!job.ids]
    reg$defs$problem = rmlevel(reg$defs$problem, nn)
  }

  sweepRegistry(reg)
  invisible(TRUE)
}

#' @export
#' @rdname ProblemAlgorithm
getProblemIds = function(reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  levels(reg$defs$problem)
}

getProblemURI = function(reg, name) {
  file.path(reg$file.dir, "problems", sprintf("%s.rds", digest(name)))
}

#' @rdname ProblemAlgorithm
#' @export
addAlgorithm = function(name, fun = NULL, reg = getDefaultRegistry())  {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name, min.chars = 1L)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, instance, ...) instance
  } else {
    assert(checkFunction(fun, args = c("job", "data", "instance")), checkFunction(fun, args = "..."))
  }

  algo = setClasses(list(fun = fun, name = name), "Algorithm")
  writeRDS(algo, file = getAlgorithmURI(reg, name))
  reg$defs$algorithm = addlevel(reg$defs$algorithm, name)
  saveRegistry(reg)
  invisible(algo)
}

#' @export
#' @rdname ProblemAlgorithm
removeAlgorithms = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE, running.ok = FALSE)
  assertCharacter(name, any.missing = FALSE)
  assertSubset(name, levels(reg$defs$algorithm))

  algorithm = NULL
  for (nn in name) {
    def.ids = reg$defs[algorithm == nn, "def.id", with = FALSE]
    job.ids = filter(def.ids, reg$status, "job.id")

    info("Removing Algorithm '%s' and %i corresponding jobs ...", nn, nrow(job.ids))
    file.remove(getAlgorithmURI(reg, nn))
    reg$defs = reg$defs[!def.ids]
    reg$status = reg$status[!job.ids]
    reg$defs$algorithm = rmlevel(reg$defs$algorithm, nn)
  }

  sweepRegistry(reg)
  invisible(TRUE)
}

#' @export
#' @rdname ProblemAlgorithm
getAlgorithmIds = function(reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  levels(reg$defs$algorithm)
}

getAlgorithmURI = function(reg, name) {
  file.path(reg$file.dir, "algorithms", sprintf("%s.rds", digest(name)))
}
