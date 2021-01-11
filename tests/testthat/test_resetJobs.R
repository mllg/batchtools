test_that("resetJobs", {
  reg = makeTestRegistry()
  f = function(x, .job) if (x == 2) stop(2) else .job$external.dir
  batchMap(f, 1:3, reg = reg)
  before = list(
    status = copy(reg$status),
    defs = copy(reg$defs)
  )
  submitAndWait(reg, 1:3)

  expect_file_exists(getLogFiles(reg, 3))
  expect_false(identical(reg$status$submitted, before$status$submitted))
  expect_file_exists(getResultFiles(reg, 1))
  expect_equal(unname(fs::dir_exists(fs::path(reg$file.dir, "external", 1:3))), c(TRUE, FALSE, TRUE))

  resetJobs(1, reg = reg)
  expect_true(all.equal(before$status[1], reg$status[1]))
  expect_false(fs::file_exists(getResultFiles(reg, 1)))
  expect_true(fs::file_exists(getResultFiles(reg, 3)))
  expect_file_exists(getLogFiles(reg, 3))
  expect_equal(unname(fs::dir_exists(fs::path(reg$file.dir, "external", 1:3))), c(FALSE, FALSE, TRUE))
  expect_false(fs::file_exists(getResultFiles(reg, 1)))
  expect_file_exists(getResultFiles(reg, 3))

  resetJobs(2:3, reg = reg)
  expect_data_table(reg$status, key = "job.id")
  expect_data_table(reg$defs, key = "def.id")
  expect_equivalent(before$status, reg$status)
  expect_false(fs::file_exists(getLogFiles(reg, 3)))
  expect_false(fs::file_exists(getResultFiles(reg, 3)))
  expect_equal(unname(fs::dir_exists(fs::path(reg$file.dir, "external", 1:3))), c(FALSE, FALSE, FALSE))
})


test_that("functions produce error after resetting jobs", {
  reg = makeTestRegistry()
  f = function(x, .job) if (x == 2) stop(2) else .job$external.dir
  batchMap(f, 1:3, reg = reg)
  submitAndWait(reg, 1:3)

  resetJobs(1, reg = reg)
  expect_error(getLog(1, reg = reg), "not available")
  expect_error(loadResult(1, reg = reg), "not terminated")
})
