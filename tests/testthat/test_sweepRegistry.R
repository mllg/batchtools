context("sweepRegistry")

test_that("sweepRegistry", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  array.jobs = isTRUE(reg$default.resources$chunks.as.arrayjobs)
  batchMap(identity, 1, reg = reg)

  submitAndWait(reg, 1, resources = list(foo = 1))
  submitAndWait(reg, 1, resources = list(foo = 2))
  writeRDS(makeJobCollection(1, reg = reg), fp(reg$file.dir, "jobs", "test.rds"))

  expect_data_table(reg$resources, nrow = 2)
  expect_character(list.files(dir(reg, "logs")), len = 2L)
  expect_character(list.files(fp(reg$file.dir, "jobs"), pattern = "\\.rds$"), len = 1L + (array.jobs && reg$cluster.functions$store.job) * 2L)
  expect_character(list.files(fp(reg$file.dir, "jobs"), pattern = "\\.job$"), len = (batchtools$debug && array.jobs) * 2L)

  expect_true(sweepRegistry(reg))

  expect_data_table(reg$resources, nrow = 1)
  expect_character(list.files(dir(reg, "logs")), len = 1L)
  if (reg$cluster.functions$store.job)
    expect_character(list.files(fp(reg$file.dir, "jobs")), len = 0L)
  checkTables(reg)


  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data))
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  addExperiments(prob.designs = list(p1 = data.table(i = 1:10)), reg = reg)
  addJobTags(6:10, "foo", reg = reg)
  expect_data_table(reg$tags, nrow = 5, any.missing = FALSE)
  removeExperiments(ids = 6:10, reg = reg)
  expect_data_table(reg$tags, nrow = 0)

  checkTables(reg)
})

test_that("relative paths work (#113)", {
  skip_if_not(identical(Sys.getenv("R_EXPENSIVE_EXAMPLE_OK"), "1"))
  fd = sprintf("~/batchtools-test-%s", basename(tempfile("")))
  reg = makeExperimentRegistry(file.dir = fd, make.default = FALSE)

  problems = list("a", "b")
  pdes = lapply(problems, function(p) {
    addProblem(name = p, data = p, fun = function(...) list(...), reg = reg)
    res = data.frame(fold = 1:3)
  })
  names(pdes) = problems

  algo.rep1 = function(job, data, instance, x) { rep(paste(data, x), instance$fold) }
  algo.rep2 = function(job, data, instance, x) { rep(paste(data, x), instance$fold) }
  addAlgorithm(name = "rep1", fun = algo.rep1, reg = reg)
  addAlgorithm(name = "rep2", fun = algo.rep2, reg = reg)

  ades = list(
    rep1 = data.table(x = LETTERS[1:3]),
    rep2 = data.table(x = letters[1:3])
  )

  addExperiments(pdes, ades, reg = reg)
  submitAndWait(reg = reg)

  ids.rep1 = findExperiments(algo.name = "rep1", reg = reg)
  ids.rep2 = findExperiments(algo.name = "rep2", reg = reg)
  removeExperiments(ids.rep2, reg = reg)

  expect_character(getLog(ids.rep1[1], reg = reg), min.len = 1, any.missing = FALSE)
  expect_list(reduceResultsList(ids = ids.rep1, reg = reg), len = 18)

  unlink(fd, recursive = TRUE)

  checkTables(reg)
})
