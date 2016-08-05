#' @title Jobs and Experiments
#'
#' @description
#' Jobs and Experiments are abstract objects which hold all information necessary to execute a single computational
#' job for a \code{\link{Registry}} or \code{\link{ExperimentRegistry}}, respectively.
#'
#' They can be created using the constructor \code{makeJob} which takes a single job id.
#' Jobs and Experiments are passed to reduce functions like \code{\link{reduceResults}}.
#' Furthermore, Experiments can be used in the functions of the \code{\link{Problem}} and \code{\link{Algorithm}}.
#' Jobs and Experiments hold these information:
#' \describe{
#'  \item{\code{job.id}}{Job ID as integer.}
#'  \item{\code{pars}}{Job parameters as named list. For \code{\link{ExperimentRegistry}}, the parameters are divided into the
#'    sublists \dQuote{prob.pars} and \dQuote{algo.pars}.}
#'  \item{\code{seed}}{Seed which is set via \code{\link{doJobCollection}}.}
#'  \item{\code{resources}}{Computational resources which were set for this job.}
#'  \item{\code{fun}}{Job only: User function passed to \code{\link{batchMap}}.}
#'  \item{\code{prob.name}}{Experiments only: Problem id.}
#'  \item{\code{algo.name}}{Experiments only: Algorithm id.}
#'  \item{\code{problem}}{Experiments only: \code{\link{Problem}}.}
#'  \item{\code{instance}}{Experiments only: Problem instance.}
#'  \item{\code{algorithm}}{Experiments only: \code{\link{Algorithm}}.}
#'  \item{\code{repl}}{Experiments only: Replication number.}
#' }
#'
#' Note that the slots \dQuote{pars}, \dQuote{fun}, \dQuote{algorithm} and \dQuote{problem}
#' lazy-load required files from the file system and construct the object on the first access.
#' Each subsequent access returns a cached version from memory (except \dQuote{instance} which is not cached).
#'
#' Jobs and Experiments be executed with \code{\link{execJob}}.
#' @template id
#' @param cache [\code{Cache}]\cr
#'  Cache to retrieve files. Used internally.
#' @template reg
#' @return [\code{Job} | \code{Experiment}].
#' @aliases Job Experiment
#' @rdname JobExperiment
#' @export
#' @examples
#' reg = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(identity, 1:5, reg = reg)
#' job = makeJob(1, reg = reg)
#' names(job)
makeJob = function(id, cache = NULL, reg = getDefaultRegistry()) {
  UseMethod("makeJob", object = reg)
}

#' @export
makeJob.Registry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = inner_join(reg$defs, reg$status[convertId(reg, id)])
  Job$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    resources = inner_join(reg$resources, row)$resources)
}

#' @export
makeJob.ExperimentRegistry = function(id, cache = NULL, reg = getDefaultRegistry()) {
  row = inner_join(reg$defs, reg$status[convertId(reg, id)])
  Experiment$new(cache %??% Cache$new(reg$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(reg$seed, row$job.id),
    repl = row$repl, resources = inner_join(reg$resources, row)$resources, prob.name = row$problem, algo.name = row$algorithm)
}

getJob = function(jc, id, cache = NULL) {
  UseMethod("getJob")
}

getJob.JobCollection = function(jc, id, cache = NULL) {
  row = inner_join(jc$defs, castIds(id))
  Job$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    resources = jc$resources)
}

getJob.ExperimentCollection = function(jc, id, cache = NULL) {
  row = inner_join(jc$defs, castIds(id))
  Experiment$new(cache %??% Cache$new(jc$file.dir), id = row$job.id, pars = row$pars[[1L]], seed = getSeed(jc$seed, row$job.id),
    repl = row$repl, resources = jc$resources, prob.name = row$problem, algo.name = row$algorithm)
}
