context("chunkIds")

test_that("chunkIds", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data, ...) nrow(data), seed = 42)
  prob = addProblem(reg = reg, "p2", data = iris, fun = function(job, data, ...) nrow(data), seed = 42)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, sq, ...) instance^sq)
  prob.designs = list(p1 = data.table(), p2 = data.table(x = 1:2))
  algo.designs = list(a1 = data.table(sq = 1:3))
  repls = 10
  ids = addExperiments(prob.designs, algo.designs, repls = repls, reg = reg)
  ids = getJobPars(reg = reg)[, c("job.id", "problem")]

  expect_error(suppressWarnings(chunkIds(1:3, chunk.size = 1, n.chunks = 1, reg = reg)), "either")

  res = suppressWarnings(chunkIds(ids, n.chunks = 1, reg = reg))
  expect_data_table(res, ncol = 2L, any.missing = FALSE, key = "job.id")
  expect_set_equal(names(res), c("job.id", "chunk"))
  expect_true(all(res$chunk == 1))

  res = suppressWarnings(chunkIds(ids, n.chunks = 1, group.by = "problem", reg = reg))
  expect_data_table(res, ncol = 2L, any.missing = FALSE, key = "job.id")
  expect_set_equal(names(res), c("job.id", "chunk"))
  res = ids[res]
  expect_true(all(res[problem == "p1", chunk] == 1))
  expect_true(all(res[problem == "p2", chunk] == 2))

  res = suppressWarnings(chunkIds(ids, chunk.size = 10, group.by = "problem", reg = reg))
  res = ids[res]
  tab = table(res$problem, res$chunk)
  expect_equal(as.numeric(tab[1, ]), rep(c(10, 0), c(3, 6)))
  expect_equal(as.numeric(tab[2, ]), rep(c(0, 10), c(3, 6)))
})

test_that("chunkIds with defaults", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, 1:10, reg = reg)
  expect_error(suppressWarnings(chunkIds(reg = reg)))
})
