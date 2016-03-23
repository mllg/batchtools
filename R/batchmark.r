#' @title Run Machine Learning Benchmarks as Experiments
#' @description
#' This function is a very parallel version of \code{\link[mlr]{benchmark}}.
#' Experiments are created in the provided registry for each combination of
#' learners, tasks and resamplings.
#'
#' @param learners [list of \link[mlr]{Learner}]\cr
#'  See \link[mlr]{Learner}].
#' @param tasks [list of \link[mlr]{Task}]\cr
#'  See \link[mlr]{Task}].
#' @param resamplings [list of \link[mlr]{ResampleDesc} or \link[mlr]{ResampleInstance}]\cr
#'  See \link[mlr]{ResampleDesc} or \link[mlr]{ResampleInstance}.
#'  One resampling strategy must be provided per task.
#' @param measures [list of \link[mlr]{Measure}]\cr
#'  See \link[mlr]{Measure}.
#' @param repls [code{integer(1)}]\cr
#'  Number of replications for each experiment.
#' @param save.models [code{logical(1)}]\cr
#'  Store (potentially large) models?
#' @template reg
#' @return [\code{data.table}]. Generated job ids are stored in the column \dQuote{job.id}.
#' @export
batchmark = function(learners, tasks, resamplings, measures = NULL, repls = 1L, save.models = FALSE, reg = getDefaultRegistry()) {
  if (!requireNamespace("mlr", quietly = TRUE))
    stop("batchmark requires the package 'mlr' to be installed")
  assertRegistry(reg)
  assertList(learners, types = "Learner", min.len = 1L)
  learner.ids = vcapply(learners, "[[", "id")
  if (anyDuplicated(learner.ids))
    stop("Duplicated learner ids found")
  assertList(tasks, types = "Task", min.len = 1L)
  task.ids = vcapply(tasks, mlr::getTaskId)
  if (anyDuplicated(task.ids))
    stop("Duplicated task ids found")
  assertList(resamplings, c("ResampleDesc", "ResampleInstance"), len = length(tasks))

  if (is.null(measures)) {
    measures = list(mlr::getDefaultMeasure(tasks[[1L]]))
  } else {
    assertList(measures, types = "Measure", min.len = 1L)
  }
  assertCount(repls)
  assertFlag(save.models)

  reg$packages = union(reg$packages, "mlr")

  # generate problems
  pdes = Map(function(id, task, rdesc, seed) {
    addProblem(id, data = list(rdesc = rdesc, task = task), fun = resample.fun, seed = seed, reg = reg)
    data.table(i = seq_len(rdesc$iters))
  }, id = task.ids, task = tasks, rdesc = resamplings, seed = reg$seed + seq_along(tasks))

  # generate algos
  ades = Map(function(id, learner) {
    apply.fun = getAlgoFun(learner, measures, save.models)
    addAlgorithm(id, apply.fun, reg = reg)
    data.table()
  }, id = learner.ids, learner = learners)

  # add experiments
  addExperiments(reg, prob.designs = pdes, algo.designs = ades, repls = repls)
}

resample.fun = function(job, data, i) {
  rin = mlr::makeResampleInstance(desc = data$rdesc, task = data$task)
  list(train = rin$train.inds[[i]], test = rin$test.inds[[i]])
}

getAlgoFun = function(lrn, measures, save.models) {
  force(lrn)
  force(measures)
  force(save.models)
  function(job, data, instance) {
    model = mlr::train(learner = lrn, task = data$task, subset = instance$train)
    pred = predict(model, task = data$task, subset = instance$test)
    perf = as.list(mlr::performance(pred, measures, task = data$task, model = model))
    if (save.models) c(list(model = model), perf) else perf
  }
}
