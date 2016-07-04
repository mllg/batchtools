context("resetJobs")

test_that("resetJobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  f = function(x) if (x == 2) stop(2) else x
  batchMap(f, 1:3, reg = reg)
  before = list(
    status = copy(reg$status),
    defs = copy(reg$defs)
  )
  silent({
    submitJobs(chunkIds(1:3, reg = reg), reg = reg)
    waitForJobs(reg = reg)
  })

  expect_true(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3), job.hash]))))
  expect_false(identical(reg$status$submitted, before$status$submitted))
  expect_true(file.exists(file.path(reg$file.dir, "results", "1.rds")))

  resetJobs(1, reg = reg)
  expect_true(all.equal(before$status[1], reg$status[1]))
  expect_false(file.exists(file.path(reg$file.dir, "results", "1.rds")))
  expect_true(file.exists(file.path(reg$file.dir, "results", "3.rds")))
  expect_true(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3), job.hash]))))
  expect_character(all.equal(before$status, reg$status))

  resetJobs(2:3, reg = reg)
  expect_true(all.equal(before$status, reg$status))
  expect_false(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3), job.hash]))))
  expect_false(file.exists(file.path(reg$file.dir, "results", "3.rds")))
})
