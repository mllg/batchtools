context("Tags")

test_that("tags work", {
  reg = makeTestRegistry()
  batchMap(identity, 1:10, reg = reg)
  expect_equal(getUsedJobTags(reg = reg), character())
  expect_data_table(findTagged(tag = "foo", reg = reg), nrow = 0, ncol = 1)
  expect_data_table(removeJobTags(reg = reg, tags = "foo"), nrow = 0, ncol = 1)

  expect_data_table(addJobTags(1:4, "walltime", reg = reg), nrow = 4, key = "job.id")
  expect_data_table(addJobTags(3:7, "broken", reg = reg), nrow = 5, key = "job.id")
  expect_set_equal(getUsedJobTags(reg = reg), c("walltime", "broken"))
  expect_set_equal(getUsedJobTags(1:2, reg = reg), c("walltime"))

  addJobTags(tags = c("foo", "bar"), reg = reg)
  x = getJobTags(reg = reg)
  expect_true(all(stri_detect_fixed(x$tags, "foo")))
  expect_true(all(stri_detect_fixed(x$tags, "bar")))
  x = removeJobTags(tags = c("foo", "bar"), reg = reg)
  expect_data_table(x, ncol = 1, nrow = 10, key = "job.id")
  x = getJobTags(reg = reg)
  expect_false(any(stri_detect_fixed(x$tags, "foo"), na.rm = TRUE))
  expect_false(any(stri_detect_fixed(x$tags, "bar"), na.rm = TRUE))

  x = getJobTags(reg = reg)
  expect_data_table(x, nrow = 10, ncol = 2, key = "job.id")
  expect_character(x$tags, min.len = 1L)

  x = findTagged(tags = "broken", reg = reg)
  expect_data_table(x, nrow = 5, ncol = 1, key = "job.id")
  expect_equal(x$job.id, 3:7)

  x = findTagged(tags = "whoops", reg = reg)
  expect_data_table(x, nrow = 0, ncol = 1, key = "job.id")

  x = removeJobTags(9:3, "walltime", reg = reg)
  expect_data_table(x, ncol = 1, nrow = 2, key = "job.id")
  expect_equal(x$job.id, 3:4)
  x = getJobTags(reg = reg)
  expect_equal(x$tags, c(rep("walltime", 2), rep("broken", 5), rep(NA_character_, 3)))

  checkTables(reg)
})
