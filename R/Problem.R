#' @title Define Problems for Experiments
#'
#' @description
#' Problems may consist of up to two parts: A static, immutable part (\code{data} in \code{addProblem})
#' and a dynamic, stochastic part (\code{fun} in \code{addProblem}).
#' For example, for statistical learning problems a data frame would be the static problem part while
#' a resampling function would be the stochastic part which creates problem instance.
#' This instance is then typically passed to a learning algorithm like a wrapper around a statistical model
#' (\code{fun} in \code{\link{addAlgorithm}}).
#'
#' This function serialize all components to the file system and registers the problem in the \code{\link{ExperimentRegistry}}.
#'
#' \code{removeProblem} removes all jobs from the registry which depend on the specific problem.
#' \code{reg$problems} holds the IDs of already defined problems.
#'
#' @param name [\code{character(1)}]\cr
#'   Unique identifier for the problem.
#' @param data [\code{ANY}]\cr
#'   Static problem part. Default is \code{NULL}.
#' @param fun [\code{function}]\cr
#'   The function defining the stochastic problem part.
#'   The static part is passed to this function with name \dQuote{data} and the \code{\link{Job}}/\code{\link{Experiment}}
#'   is passed as \dQuote{job}.
#'   Therefore, your function must have the formal arguments \dQuote{job} and \dQuote{data} (or dots \code{...}).
#'   If you do not provide a function, it defaults to a function which just returns the data part.
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
#' @aliases Problem
#' @seealso \code{\link{Algorithm}}, \code{\link{addExperiments}}
#' @export
#' @examples
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#' addProblem("p1", fun = function(job, data) data, reg = tmp)
#' addProblem("p2", fun = function(job, data) job, reg = tmp)
#' addAlgorithm("a1", fun = function(job, data, instance) instance, reg = tmp)
#'
#' tmp$problems
#' tmp$algorithms
#'
#' removeProblems("p1", reg = tmp)
#'
#' tmp$problems
#' tmp$algorithms
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
  if (!is.null(seed)) {
    seed = asCount(seed, positive = TRUE)
  }

  info("Adding problem '%s'", name)
  prob = setClasses(list(name = name, seed = seed, data = data, fun = fun), "Problem")
  writeRDS(prob, file = getProblemURI(reg, name))
  reg$problems = union(reg$problems, name)
  saveRegistry(reg)
  invisible(prob)
}

#' @export
#' @rdname addProblem
removeProblems = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE, running.ok = FALSE)
  assertCharacter(name, any.missing = FALSE)
  assertSubset(name, reg$problems)

  problem = NULL
  for (nn in name) {
    def.ids = reg$defs[problem == nn, "def.id"]
    job.ids = filter(def.ids, reg$status, "job.id")

    info("Removing Problem '%s' and %i corresponding jobs ...", nn, nrow(job.ids))
    file.remove.safely(getProblemURI(reg, nn))
    reg$defs = reg$defs[!def.ids]
    reg$status = reg$status[!job.ids]
    reg$problems = chsetdiff(reg$problems, nn)
  }

  sweepRegistry(reg)
  invisible(TRUE)
}
