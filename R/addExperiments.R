#' @title Add Experiments to the Registry
#'
#' @description
#' Adds experiments (parametrized combinations of problems with algorithms) to the registry and thereby defines batch jobs.
#'
#' If multiple problem designs or algorithm designs are provided, they are combined via the Cartesian product.
#' E.g., if you have two problems \code{p1} and \code{p2} and three algorithms \code{a1}, \code{a2} and \code{a3},
#' \code{addExperiments} creates experiments for all parameters for the combinations \code{(p1, a1)}, \code{(p1, a2)},
#' \code{(p1, a3)}, \code{(p2, a1)}, \code{(p2, a2)} and \code{(p2, a3)}.
#'
#' @param prob.designs [named list of \code{\link[base]{data.frame}}]\cr
#'   Named list of data frames (or \code{\link[data.table]{data.table}}).
#'   The name must match the problem name while the column names correspond to parameters of the problem.
#'   If \code{NULL}, experiments for all defined problems without any parameters are added.
#' @param algo.designs [named list of \code{\link[data.table]{data.table}} or \code{\link[base]{data.frame}}]\cr
#'   Named list of data frames (or \code{\link[data.table]{data.table}}).
#'   The name must match the algorithm name while the column names correspond to parameters of the algorithm.
#'   If \code{NULL}, experiments for all defined algorithms without any parameters are added.
#' @param repls [\code{integer(1)}]\cr
#'   Number of replications for each experiment.
#' @param combine [\code{character(1)}]\cr
#'   How to combine the rows of a single problem design with the rows of a single algorithm design?
#'   Default is \dQuote{crossprod} which combines each row of the problem design which each row of the algorithm design
#'   in a cross-product fashion. Set to \dQuote{bind} to just \code{\link[base]{cbind}} the tables of
#'   problem and algorithm designs where the shorter table is repeated if necessary.
#' @template expreg
#' @return [\code{\link{data.table}}] with ids of added jobs stored in column \dQuote{job.id}.
#' @export
#' @family Experiment
#' @examples
#' tmp = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
#'
#' # add first problem
#' fun = function(job, data, n, mean, sd, ...) rnorm(n, mean = mean, sd = sd)
#' addProblem("p1", fun = fun, reg = tmp)
#'
#' # add second problem
#' fun = function(job, data, n, lamba, ...) rexp(n, lambda = lambda)
#' addProblem("p2", fun = fun, reg = tmp)
#'
#' # add first algorithm
#' fun = function(instance, method, ...) if (method == "mean") mean(instance) else median(instance)
#' addAlgorithm("a1", fun = fun, reg = tmp)
#'
#' # add second algorithm
#' fun = function(instance, ...) se(instance)
#' addAlgorithm("a2", reg = tmp)
#'
#' # define problem and algorithm designs
#' prob.designs = algo.designs = list()
#' prob.designs$p1 = expand.grid(n = 100, mean = -1:1, sd = 1:5)
#' prob.designs$p2 = data.table(lambda = 1:5)
#' algo.designs$a1 = data.table(method = c("mean", "median"))
#' algo.designs$a2 = data.table()
#'
#' # add experiments
#' addExperiments(prob.designs, algo.designs, reg = tmp)
#'
#' # check what has been created
#' summarizeExperiments(reg = tmp)
#' getJobPars(reg = tmp)
addExperiments = function(prob.designs = NULL, algo.designs = NULL, repls = 1L, combine = "crossprod", reg = getDefaultRegistry()) {
  convertDesigns = function(type, designs, keywords) {
    Map(function(id, design) {
      design = as.data.table(design)
      i = wf(keywords %chin% names(design))
      if (length(i) > 0L)
        stopf("%s design %s contains reserved keyword '%s'", type, id, keywords[i])
      design
    }, id = names(designs), design = designs)
  }

  assertExperimentRegistry(reg, writeable = TRUE)
  if (is.null(prob.designs)) {
    probs = levels(reg$defs$problem)
    prob.designs = replicate(length(probs), data.table(), simplify = FALSE)
    names(prob.designs) = probs
  } else {
    assertList(prob.designs, types = "data.frame", names = "named")
    assertSubset(names(prob.designs), levels(reg$defs$problem))
    prob.designs = convertDesigns("Problem", prob.designs, c("job", "data"))
  }
  if (is.null(algo.designs)) {
    algos = levels(reg$defs$algorithm)
    algo.designs = replicate(length(algos), data.table(), simplify = FALSE)
    names(algo.designs) = algos
  } else {
    assertList(algo.designs, types = "data.frame", names = "named")
    assertSubset(names(algo.designs), levels(reg$defs$algorithm))
    algo.designs = convertDesigns("Algorithm", algo.designs, c("job", "data", "instance"))
  }
  repls = asCount(repls)
  assertChoice(combine, c("crossprod", "bind"))

  all.ids = integer(0L)

  for (i in seq_along(prob.designs)) {
    pn = names(prob.designs)[i]
    pd = prob.designs[[i]]
    n.pd = max(nrow(pd), 1L)

    for (j in seq_along(algo.designs)) {
      an = names(algo.designs)[j]
      ad = algo.designs[[j]]
      n.ad = max(nrow(ad), 1L)

      if (combine == "crossprod") {
        n.jobs = n.pd * n.ad * repls
        info("Adding %i experiments ('%s'[%i] x '%s'[%i] x repls[%i]) ...", n.jobs, pn, n.pd, an, n.ad, repls)
        idx = CJ(.i = seq_len(n.pd), .j = seq_len(n.ad))
      } else {
        n.jobs = max(n.pd, n.ad) * repls
        info("Adding %i experiments (('%s'[%i] | '%s'[%i]) x repls[%i]) ...", n.jobs, pn, n.pd, an, n.ad, repls)
        idx = data.table(.i = rep_len(seq_len(n.pd), n.jobs), .j = rep_len(seq_len(n.ad), n.jobs))
      }

      # create temp tab with prob name, algo name and pars as list
      tab = data.table(
        pars = Map(function(pp, ap) list(prob.pars = pp, algo.pars = ap),
          pp = if (nrow(pd) > 0L) .mapply(list, pd[idx$.i], list()) else list(list()),
          ap = if (nrow(ad) > 0L) .mapply(list, ad[idx$.j], list()) else list(list())),
        problem = pn,
        algorithm = an)

      # create hash of each row of tab
      tab$pars.hash = unlist(.mapply(function(...) digest(list(...)), tab, list()))

      # merge with already defined experiments to get def.ids
      tab = merge(reg$defs[, !c("pars", "problem", "algorithm")], tab, by = "pars.hash", all.x = FALSE, all.y = TRUE, sort = FALSE)

      # generate def ids for new experiments
      w = which(is.na(tab$def.id))
      if (length(w) > 0L) {
        tab[w, "def.id" := auto_increment(reg$defs$def.id, length(w))]
        reg$defs = rbind(reg$defs, tab[w])
      }

      # create rows in status table for new defs and each repl and filter for defined
      tab = CJ(def.id = tab$def.id, repl = seq_len(repls))[!reg$status, on = c("def.id", "repl")]
      if (nrow(tab) < n.jobs)
        info("Skipping %i duplicated experiments ...", n.jobs - nrow(tab))

      if (nrow(tab) > 0L) {
        # rbind new status
        tab$job.id = auto_increment(reg$status$job.id, nrow(tab))
        reg$status = rbind(reg$status, tab, fill = TRUE)
      }

      all.ids = c(all.ids, tab$job.id)
    }
  }

  setkeyv(reg$defs, "def.id")
  setkeyv(reg$status, "job.id")
  saveRegistry(reg)
  invisible(data.table(job.id = all.ids, key = "job.id"))
}
