context("getJobTable")

test_that("getJobTable.Registry", {
  reg = makeTestRegistry()
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:4, j = rep(1, 4), reg = reg)

  tab = getJobTable(reg = reg)
  expect_data_table(tab, nrows = 4, ncols = 15, key = "job.id")
  expect_list(tab$job.pars)
  expect_equal(tab$job.pars[[1]], list(i = 1L, j = 1))

  tab = flatten(tab)
  expect_data_table(tab, nrows = 4, ncols = 15, key = "job.id")
  expect_null(tab[["job.pars"]])
  expect_equal(tab$i, 1:4)
  expect_equal(tab$j, rep(1, 4))

  expect_is(tab$submitted, "POSIXct")
  expect_is(tab$started, "POSIXct")
  expect_is(tab$done, "POSIXct")
  expect_is(tab$time.queued, "difftime")
  expect_numeric(tab$time.queued, lower = 0)
  expect_is(tab$time.running, "difftime")
  expect_numeric(tab$time.running, lower = 0)
  expect_character(tab$tags)
  expect_true(allMissing(tab$tags))

  tab = flatten(getJobTable(reg = reg), sep = ".")
  expect_null(tab[["job.pars"]])
  expect_equal(tab$job.pars.i, 1:4)
  expect_equal(tab$job.pars.j, rep(1, 4))

  # be sure that the original tables are untouched
  checkTables(reg)

  submitAndWait(reg = reg, ids = s.chunk(ids), resources = list(my.walltime = 42L))
  addJobTags(2:3, "my_tag", reg = reg)

  tab = getJobTable(reg = reg)
  expect_data_table(tab, key = "job.id")
  expect_copied(tab, reg$status)
  expect_is(tab$submitted, "POSIXct")
  expect_is(tab$started, "POSIXct")
  expect_is(tab$done, "POSIXct")
  expect_is(tab$time.queued, "difftime")
  expect_numeric(tab$time.queued, lower = 0)
  expect_is(tab$time.running, "difftime")
  expect_numeric(tab$time.running, lower = 0)
  expect_character(tab$tags, min.len = 1L)

  tab = getJobResources(reg = reg)
  expect_data_table(tab, nrow = 4, ncols = 2, key = "job.id")
  expect_copied(tab, reg$resources)
  expect_set_equal(tab$resource.hash[1], tab$resource.hash)
  expect_list(tab$resources)
  expect_true(all(vlapply(tab$resources, function(r) r$my.walltime == 42)))

  tab = flatten(getJobResources(reg = reg))
  expect_null(tab[["resources"]])
  expect_integer(tab$my.walltime, any.missing = FALSE)
})

test_that("getJobPars", {
  reg = makeTestRegistry()
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:4, j = rep(1, 4), reg = reg)
  tab = getJobPars(reg = reg)
  expect_data_table(tab, nrow = 4, ncol = 2, key = "job.id")
  tab = flatten(tab)
  expect_copied(tab, reg$defs)
  expect_null(tab$job.pars)
  expect_equal(tab$i, 1:4)
  expect_equal(tab$j, rep(1, 4))
  tab = flatten(getJobPars(reg = reg, ids = 1:2))
  expect_data_table(tab, nrow = 2, ncol = 3, key = "job.id")
  tab = flatten(getJobPars(reg = reg), sep = ".")
  expect_data_table(tab, nrow = 4, ncol = 3, key = "job.id")
  expect_equal(tab$job.pars.i, 1:4)
  expect_equal(tab$job.pars.j, rep(1, 4))
})

test_that("getJobPars with repls", {
  reg = makeTestExperimentRegistry()
  prob = addProblem("prob", data = iris, fun = function(data, job) nrow(data), reg = reg)
  algo = addAlgorithm("algo", fun = function(job, data, instance, i, ...) instance, reg = reg)
  prob.designs = list(prob = data.table())
  algo.designs = list(algo = data.table(i = 1:2))
  ids = addExperiments(prob.designs, algo.designs, repls = 3, reg = reg)
  waitForJobs(reg = reg, sleep = 1)
  ids[, chunk := chunk(job.id, chunk.size = 2)]
  submitAndWait(ids = ids, reg = reg)
  expect_equal(nrow(getJobPars(reg = reg)), nrow(ids))
})

test_that("getJobTable.ExperimentRegistry", {
  reg = makeTestExperimentRegistry()
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  ids = addExperiments(list(p1 = data.table(k = 1)), list(a1 = data.table(sq = 1:3)), reg = reg)

  tab = getJobTable(reg = reg)
  expect_data_table(tab, nrows = 3, ncols = 19, key = "job.id")
  expect_copied(tab, reg$status)
  expect_null(tab$job.pars)
  expect_list(tab$prob.pars)
  expect_list(tab$algo.pars)
  for (i in 1:3) {
    expect_equal(tab$prob.pars[[i]], list(k = 1))
    expect_equal(tab$algo.pars[[i]], list(sq = i))
  }
  expect_equal(tab$problem[1], "p1")
  expect_equal(tab$algorithm[1], "a1")

  tab = flatten(getJobTable(ids = 1:3, reg = reg), c("prob.pars", "algo.pars"))
  expect_data_table(tab, nrows = 3, ncols = 19, key = "job.id")
  expect_null(tab[["job.pars"]])
  expect_set_equal(tab$k, rep(1, 3))
  expect_set_equal(tab$sq, 1:3)

  tab = flatten(getJobPars(reg = reg), sep = ".")
  expect_null(tab[["job.pars"]])
  expect_set_equal(tab$prob.pars.k, rep(1, 3))
  expect_set_equal(tab$algo.pars.sq, 1:3)
})


test_that("experiment registry with vector parameters", {
  tmp = makeTestExperimentRegistry()
  fun = function(job, data, n, mean, sd, ...) rnorm(sum(n), mean = mean, sd = sd)
  addProblem("rnorm", fun = fun, reg = tmp)
  fun = function(instance, ...) sd(instance)
  addAlgorithm("deviation", fun = fun, reg = tmp)

  prob.designs = algo.designs = list()
  prob.designs$rnorm = data.table(expand.grid(n = list(100, 1:4), mean = 0, sd = 1:2))
  algo.designs$deviation = data.table()
  addExperiments(prob.designs, algo.designs, reg = tmp)
  submitAndWait(reg = tmp)

  res = getJobPars(reg = tmp)
  expect_data_table(res, ncol = 5)
  expect_list(res$prob.pars, len = 4)
  res = flatten(res)
  expect_data_table(res, ncol = 6, nrow = 4, col.names = "unique")
  expect_list(res$n, len = 4)
  expect_numeric(res$mean, len = 4, any.missing = FALSE)
  expect_numeric(res$sd, len = 4, any.missing = FALSE)

  res = flatten(res)
  expect_data_table(res, ncol = 9, nrow = 4, col.names = "unique")
})
