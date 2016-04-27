context("hooks")

test_that("hooks", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$cluster.functions$hooks = list(
    "collection.start" = function(jc, con, ...) cat(jc$job.hash, "\n", sep = "", file = con),
    "post.sync" = function(reg, ...) cat("post.syn", file = file.path(tempdir(), "post.sync.txt"))
  )
  batchMap(identity, 1, reg = reg)

  jc = makeJobCollection(1, reg = reg)
  expect_function(jc$hooks$collection.start, args = "jc")

  fn.ps = file.path(tempdir(), "post.sync.txt")
  expect_false(file.exists(fn.ps))
  silent(submitJobs(1, reg = reg))
  expect_true(file.exists(fn.ps))

  lines = readLog(1, reg = reg)
  expect_true(reg$status[1]$job.hash %in% lines)

  unlink(fn.ps)
})
