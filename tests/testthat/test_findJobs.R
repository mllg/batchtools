context("findJobs")

none = noIds()

test_that("find[Status]", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)

  expect_equal(findJobs(reg = reg), none)
  expect_equal(findSubmitted(reg = reg), none)
  expect_equal(findNotSubmitted(reg = reg), none)
  expect_equal(findStarted(reg = reg), none)
  expect_equal(findNotStarted(reg = reg), none)
  expect_equal(findDone(reg = reg), none)
  expect_equal(findNotDone(reg = reg), none)
  expect_equal(findErrors(reg = reg), none)
  expect_equal(findOnSystem(reg = reg), none)
  expect_equal(findRunning(reg = reg), none)
  expect_equal(findQueued(reg = reg), none)
  expect_equal(findExpired(reg = reg), none)

  fun = function(i) if (i == 3) stop(i) else i
  ids = batchMap(fun, i = 1:5, reg = reg)
  all = reg$status[, "job.id"]

  expect_equal(findJobs(reg = reg), all)
  expect_equal(findSubmitted(reg = reg), none)
  expect_equal(findStarted(reg = reg), none)
  expect_equal(findNotStarted(reg = reg), all)
  expect_equal(findDone(reg = reg), none)
  expect_equal(findNotDone(reg = reg), all)
  expect_equal(findErrors(reg = reg), none)
  expect_equal(findOnSystem(reg = reg), none)
  expect_equal(findRunning(reg = reg), none)
  expect_equal(findQueued(reg = reg), none)
  expect_equal(findExpired(reg = reg), none)

  submitAndWait(reg, ids)

  expect_equal(findJobs(reg = reg), all)
  expect_equal(findSubmitted(reg = reg), all)
  expect_equal(findNotSubmitted(reg = reg), none)
  expect_equal(findStarted(reg = reg), all)
  expect_equal(findNotStarted(reg = reg), none)
  expect_equal(findDone(reg = reg), all[-3L])
  expect_equal(findNotDone(reg = reg), all[3L])
  expect_equal(findErrors(reg = reg), all[3L])
  expect_equal(findOnSystem(reg = reg), none)
  expect_equal(findRunning(reg = reg), none)
  expect_equal(findQueued(reg = reg), none)
  expect_equal(findExpired(reg = reg), none)
})

test_that("Subsetting", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(i) if (i == 3) stop(i) else i
  ids = batchMap(fun, i = 1:5, reg = reg)
  submitAndWait(reg, ids)
  all = reg$status[, "job.id"]

  expect_equal(findJobs(ids = 1:3, reg = reg), all[1:3])
  expect_equal(findDone(ids = 3, reg = reg), none)
  expect_equal(findErrors(ids = 1:2, reg = reg), none)
  expect_equal(findSubmitted(1:5, reg = reg), all)
  expect_data_table(findSubmitted(6, reg = reg), ncol = 1L, nrow = 0L)
})

test_that("findJobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(i, j) i + j
  ids = batchMap(fun, i = 1:5, j = c(2, 2, 3, 4, 4), reg = reg)
  all = reg$status[, "job.id"]
  expect_equal(findJobs(i == 1, reg = reg), all[1])
  expect_equal(findJobs(i >= 3, reg = reg), all[3:5])
  expect_equal(findJobs(i >= 3 & j > 3, reg = reg), all[4:5])
  xi = 2
  expect_equal(findJobs(i == xi, reg = reg), all[2])
})

test_that("findOnSystem", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  if (is.null(reg$cluster.functions$listJobsRunning))
    skip("Test requires listJobsRunning")
  silent({
    ids = batchMap(reg = reg, Sys.sleep, c(10, 10))
    submitJobs(reg = reg, ids = s.chunk(ids))
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_equal(findExpired(reg = reg), none)
    waitForJobs(reg = reg, sleep = 1)
  })
})

test_that("findExperiments", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", fun = function(job, data, n, ...) mean(runif(n)), seed = 42)
  prob = addProblem(reg = reg, "p2", data = iris, fun = function(job, data) nrow(data))
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq) instance^sq)
  prob.designs = list(p1 = data.table(n = c(10, 20)), p2 = data.table())
  algo.designs = list(a1 = data.table(sq = 1:3))
  repls = 10
  addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)

  tab = findExperiments(reg = reg)
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.name = "p1")
  expect_data_table(tab, nrow = 60, ncol = 1, key = "job.id")

  expect_error(findExperiments(reg = reg, prob.name = c("p1", "p2")), "length 1")

  tab = findExperiments(reg = reg, prob.pattern = "p.")
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.pattern = "2$")
  expect_data_table(tab, nrow = 30, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.pattern = "p1", algo.pattern = "a1", repls = 3:4)
  expect_data_table(tab, nrow = 12, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.pattern = c("^p"))
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.name = "p2")
  expect_data_table(tab, nrow = 30, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, ids = 1:10, prob.name = "p1")
  expect_data_table(tab, nrow = 10, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.name = "a1")
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.name = "p1", prob.pars = n == 10)
  expect_data_table(tab, nrow = 30, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.pars = sq == 2)
  expect_data_table(tab, nrow = 30, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.name = "a1")
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.pattern = "a.")
  expect_data_table(tab, nrow = 90, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.name = "p")
  expect_data_table(tab, nrow = 0, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.name = "a")
  expect_data_table(tab, nrow = 0, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, prob.name = "xxx")
  expect_data_table(tab, nrow = 0, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, algo.name = "xxx")
  expect_data_table(tab, nrow = 0, ncol = 1, key = "job.id")

  tab = findExperiments(reg = reg, repls = 1:2)
  expect_data_table(tab, nrow = 18, ncol = 1, key = "job.id")
})
