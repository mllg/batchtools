context("sweepRegistry")

test_that("sweepRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  array.jobs = isTRUE(reg$default.resources$chunks.as.arrayjobs)
  batchMap(identity, 1, reg = reg)

  submitAndWait(reg, 1, resources = list(foo = 1))
  submitAndWait(reg, 1, resources = list(foo = 2))
  writeRDS(makeJobCollection(1, reg = reg), file.path(reg$file.dir, "jobs", "test.rds"))

  expect_data_table(reg$resources, nrow = 2)
  expect_character(list.files(getLogPath(reg)), len = 2L)
  expect_character(list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.rds$"), len = 1L + (array.jobs && reg$cluster.functions$store.job) * 2L)
  expect_character(list.files(file.path(reg$file.dir, "jobs"), pattern = "\\.job$"), len = (batchtools$debug && array.jobs) * 2L)

  expect_true(sweepRegistry(reg))

  expect_data_table(reg$resources, nrow = 1)
  expect_character(list.files(getLogPath(reg)), len = 1L)
  if (reg$cluster.functions$store.job)
    expect_character(list.files(file.path(reg$file.dir, "jobs")), len = 0L)
  checkTables(reg)


  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  addExperiments(prob.designs = list(p1 = data.table(i = 1:10)), reg = reg)
  addJobTags(6:10, "foo", reg = reg)
  expect_data_table(reg$tags, nrow = 5, any.missing = FALSE)
  removeExperiments(ids = 6:10, reg = reg)
  expect_data_table(reg$tags, nrow = 0)
})
