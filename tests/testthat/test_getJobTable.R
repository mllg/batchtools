context("getJobTable")

test_that("getJobTable.Registry", {
  reg = makeTempRegistry(FALSE)
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:4, j = rep(1, 4), reg = reg)

  tab = getJobTable(reg = reg, pars.as.cols = FALSE, resources.as.cols = FALSE)
  expect_data_table(tab, nrows = 4, ncols = 14, key = "job.id")
  expect_list(tab$pars)
  expect_equal(tab$pars[[1]], list(i = 1L, j = 1))

  tab = getJobTable(reg = reg, pars.as.cols = TRUE)
  expect_data_table(tab, nrows = 4, ncols = 15, key = "job.id")
  expect_null(tab[["pars"]])
  expect_equal(tab$i, 1:4)
  expect_equal(tab$j, rep(1, 4))

  expect_is(tab$submitted, "POSIXct")
  expect_is(tab$started, "POSIXct")
  expect_is(tab$done, "POSIXct")
  expect_is(tab$time.queued, "difftime")
  expect_is(tab$time.running, "difftime")

  tab = getJobTable(reg = reg, pars.as.cols = TRUE, prefix = TRUE)
  expect_null(tab[["pars"]])
  expect_equal(tab$par.i, 1:4)
  expect_equal(tab$par.j, rep(1, 4))

  # be sure that the original tables are untouched
  checkTables(reg)
})

test_that("getJobResources", {
  reg = makeTempRegistry(FALSE)
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:4, j = rep(1, 4), reg = reg)
  silent({
    submitJobs(reg = reg, ids = chunkIds(ids, reg = reg), resources = list(my.walltime = 42L))
    waitForJobs(reg = reg)
  })
  tab = getJobResources(reg = reg, resources.as.cols = FALSE)
  expect_data_table(tab, nrow = 4, ncols = 3, key = "job.id")
  expect_set_equal(tab$resources.hash[1], tab$resources.hash)
  expect_list(tab$resources)
  expect_true(all(vlapply(tab$resources, function(r) r$my.walltime == 42)))

  tab = getJobResources(reg = reg, resources.as.cols = TRUE)
  expect_null(tab[["resources"]])
  expect_integer(tab$my.walltime, any.missing = FALSE)
})

test_that("getJobPars", {
  reg = makeTempRegistry(FALSE)
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:4, j = rep(1, 4), reg = reg)
  tab = getJobPars(reg = reg, pars.as.cols = NULL)
  expect_data_table(tab, nrow = 4, ncol = 3, key = "job.id")
  expect_null(tab$pars)
  expect_equal(tab$i, 1:4)
  expect_equal(tab$j, rep(1, 4))
  tab = getJobPars(reg = reg, ids = 1:2, pars.as.cols = NULL)
  expect_data_table(tab, nrow = 2, ncol = 3, key = "job.id")
  tab = getJobPars(reg = reg, prefix = TRUE, pars.as.cols = NULL)
  expect_data_table(tab, nrow = 4, ncol = 3, key = "job.id")
  expect_equal(tab$par.i, 1:4)
  expect_equal(tab$par.j, rep(1, 4))
})

test_that("getJobPars with repls", {
  reg = makeTempExperimentRegistry()
  prob = addProblem("prob", data = iris, fun = function(data, job) nrow(data), reg = reg)
  algo = addAlgorithm("algo", fun = function(job, data, instance, i, ...) instance, reg = reg)
  prob.designs = list(prob = data.table())
  algo.designs = list(algo = data.table(i = 1:2))
  addExperiments(prob.designs, algo.designs, repls = 3, reg = reg)
  waitForJobs(reg = reg)
  ids = chunkIds(chunk.size = 2, reg = reg)
  silent({
    submitJobs(ids, reg = reg)
    waitForJobs(reg = reg)
  })
  expect_equal(nrow(getJobPars(reg = reg)), nrow(ids))
})

test_that("getJobTable.ExperimentRegistry", {
  reg = makeTempExperimentRegistry(FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  ids = addExperiments(list(p1 = data.table(k = 1)), list(a1 = data.table(sq = 1:3)), reg = reg)

  tab = getJobTable(reg = reg, pars.as.cols = FALSE)
  expect_data_table(tab, nrows = 3, ncols = 17, key = "job.id")
  expect_list(tab$pars)
  expect_equal(tab$pars[[1]], list(prob.pars = list(k = 1), algo.pars = list(sq = 1)))
  expect_equal(tab$pars[[2]], list(prob.pars = list(k = 1), algo.pars = list(sq = 2)))
  expect_equal(tab$pars[[3]], list(prob.pars = list(k = 1), algo.pars = list(sq = 3)))
  expect_equal(tab$problem[1], factor("p1"))
  expect_equal(tab$algorithm[1], factor("a1"))

  tab = getJobTable(ids = 1:3, reg = reg, pars.as.cols = TRUE)
  expect_data_table(tab, nrows = 3, ncols = 18, key = "job.id")
  expect_null(tab[["pars"]])
  expect_set_equal(tab$k, rep(1, 3))
  expect_set_equal(tab$sq, 1:3)

  tab = getJobTable(reg = reg, pars.as.cols = TRUE, prefix = TRUE)
  expect_null(tab[["pars"]])
  expect_set_equal(tab$prob.par.k, rep(1, 3))
  expect_set_equal(tab$algo.par.sq, 1:3)
})


test_that("parsAsCols autodetection", {
  reg = makeTempRegistry(FALSE)
  input = list(1, NULL, iris, letters)
  batchMap(identity, x = input, reg = reg)
  tab = getJobPars(reg = reg, pars.as.cols = NULL)
  expect_data_table(tab, ncols = 2, key = "job.id")
  expect_set_equal(names(tab), c("job.id", "pars"))
})
