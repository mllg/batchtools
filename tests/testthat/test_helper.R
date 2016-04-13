context("helper")

test_that("filter", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, 1:10, reg = reg)

  tab = filter(reg$status, NULL)
  expect_data_table(tab, ncol = ncol(reg$status))
  #expect_copied(tab, reg$status)

  tab = filter(reg$status, 1:10)
  expect_data_table(tab, ncol = ncol(reg$status))
  expect_copied(tab, reg$status)

  tab = filter(reg$status, findJobs(reg = reg))
  expect_data_table(tab, ncol = ncol(reg$status))
  expect_copied(tab, reg$status)

  tab = filter(reg$status, as.data.frame(findJobs(reg = reg)))
  expect_data_table(tab, ncol = ncol(reg$status))
  expect_copied(tab, reg$status)
})
