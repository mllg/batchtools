#' @title Define Algorithms for Experiments
#'
#' @description
#' Algorithms are functions which get the code{data} part as well as the problem instance (the return value of the
#' function defined in \code{\link{Problem}}) and return an arbitrary R object.
#'
#' This function serializes all components to the file system and registers the algorithm in the \code{\link{ExperimentRegistry}}.
#'
#' \code{removeAlgorithm} removes all jobs from the registry which depend on the specific algorithm.
#' \code{reg$algorithms} holds the IDs of already defined algorithms.
#'
#' @param name [\code{character(1)}]\cr
#'   Unique identifier for the algorithm.
#' @param fun [\code{function}]\cr
#'   The algorithm function. The static problem part is passed as \dQuote{data}, the generated
#'   problem instance is passed as \dQuote{instance} and the \code{\link{Job}}/\code{\link{Experiment}} as \dQuote{job}.
#'   Therefore, your function must have the formal arguments \dQuote{job}, \dQuote{data} and \dQuote{instance} (or dots \code{...}).
#'
#'   If you do not provide a function, it defaults to a function which just returns the instance.
#' @template expreg
#' @return [\code{Algorithm}]. Object of class \dQuote{Algorithm}.
#' @aliases Algorithm
#' @seealso \code{\link{Problem}}, \code{\link{addExperiments}}
#' @export
addAlgorithm = function(name, fun = NULL, reg = getDefaultRegistry())  {
  assertRegistry(reg, class = "ExperimentRegistry", writeable = TRUE)
  assertString(name, min.chars = 1L)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, instance, ...) instance
  } else {
    assert(checkFunction(fun, args = c("job", "data", "instance")), checkFunction(fun, args = "..."))
  }

  info("Adding algorithm '%s'", name)
  algo = setClasses(list(fun = fun, name = name), "Algorithm")
  writeRDS(algo, file = getAlgorithmURI(reg, name), compress = reg$compress)
  reg$algorithms = union(reg$algorithms, name)
  saveRegistry(reg)
  invisible(algo)
}

#' @export
#' @rdname addAlgorithm
removeAlgorithms = function(name, reg = getDefaultRegistry()) {
  assertRegistry(reg, class = "ExperimentRegistry", writeable = TRUE, running.ok = FALSE)
  assertCharacter(name, any.missing = FALSE)
  assertSubset(name, reg$algorithms)

  algorithm = NULL
  for (nn in name) {
    def.ids = reg$defs[algorithm == nn, "def.id"]
    job.ids = filter(def.ids, reg$status, "job.id")

    info("Removing Algorithm '%s' and %i corresponding jobs ...", nn, nrow(job.ids))
    file_remove(getAlgorithmURI(reg, nn))
    reg$defs = reg$defs[!def.ids]
    reg$status = reg$status[!job.ids]
    reg$algorithms = chsetdiff(reg$algorithms, nn)
  }

  sweepRegistry(reg)
  invisible(TRUE)
}

getAlgorithmURI = function(reg, name) {
  fs::path(dir(reg, "algorithms"), mangle(name))
}
