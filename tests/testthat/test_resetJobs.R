context("resetJobs")

test_that("resetJobs", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  f = function(x, .job) if (x == 2) stop(2) else .job$external.dir
  batchMap(f, 1:3, reg = reg)
  before = list(
    status = copy(reg$status),
    defs = copy(reg$defs)
  )
  submitAndWait(reg, 1:3)

  expect_true(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3), job.hash]))))
  expect_false(identical(reg$status$submitted, before$status$submitted))
  expect_true(file.exists(file.path(reg$file.dir, "results", "1.rds")))
  expect_equal(dir.exists(file.path(reg$file.dir, "external", 1:3)), c(TRUE, FALSE, TRUE))

  resetJobs(1, reg = reg)
  expect_true(all.equal(before$status[1], reg$status[1]))
  expect_false(file.exists(file.path(reg$file.dir, "results", "1.rds")))
  expect_true(file.exists(file.path(reg$file.dir, "results", "3.rds")))
  expect_true(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3L), job.hash]))))
  expect_equal(dir.exists(file.path(reg$file.dir, "external", 1:3)), c(FALSE, FALSE, TRUE))

  resetJobs(2:3, reg = reg)
  expect_data_table(reg$status, key = "job.id")
  expect_data_table(reg$defs, key = "def.id")
  expect_equivalent(before$status, reg$status)
  expect_false(file.exists(file.path(reg$file.dir, "logs", sprintf("%s.log", reg$status[.(3L), job.hash]))))
  expect_false(file.exists(file.path(reg$file.dir, "results", "3.rds")))
  expect_equal(dir.exists(file.path(reg$file.dir, "external", 1:3)), c(FALSE, FALSE, FALSE))
})
