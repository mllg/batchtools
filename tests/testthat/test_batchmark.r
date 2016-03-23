context("batchmark")

test_that("batchmark", {
  requireNamespace("mlr")
  reg = makeTempExperimentRegistry(FALSE)
  learners = list(mlr::makeLearner("classif.rpart"))
  tasks = list(mlr::makeClassifTask(data = iris, target = "Species"))
  resamplings = list(mlr::makeResampleDesc("CV", iters = 3))

  ids = batchmark(learners, tasks, resamplings, reg = reg)
  expect_data_table(ids, ncol = 1L, nrow = 3, key = "job.id")
  expect_set_equal(ids$job.id, 1:3)
  tab = summarizeExperiments(reg = reg)
  expect_equal(tab$problems, "iris")
  expect_equal(tab$algorithms, "classif.rpart")
  expect_set_equal(findExperiments(reg = reg)$job.id, 1:3)

  silent({
    submitJobs(1, reg = reg)
    waitForJobs(1, reg = reg)
  })

  expect_equal(findDone(reg = reg)$job.id, 1L)
  res = loadResult(1, reg = reg)
  expect_list(res, len = 1L)
  expect_number(res$mmce)

  # same experiments
  ids = batchmark(learners, tasks, resamplings, reg = reg)
  expect_set_equal(findExperiments(reg = reg)$job.id, 1:3)

  # more repls
  ids = batchmark(learners, tasks, resamplings, repls = 2, reg = reg)
  expect_set_equal(findExperiments(reg = reg)$job.id, 1:6)

  # add a learner
  learners = c(learners, list(mlr::makeLearner("classif.ranger", num.trees = 10)))
  ids = batchmark(learners, tasks, resamplings, repls = 1, reg = reg)
  expect_set_equal(findExperiments(reg = reg)$job.id, 1:9)

  tab = summarizeExperiments(reg = reg)
  expect_equal(tab$problems, "iris")
  expect_equal(tab$algorithms, c("classif.rpart", "classif.ranger"))
  expect_set_equal(tab$table$.count, c(3L, 6L))
})
