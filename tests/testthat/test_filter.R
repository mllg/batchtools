context("filter helper")

test_that("filter", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  batchMap(identity, 1:10, reg = reg)
  reg$status = reg$status[-3, ]

  tab = filter(reg$status, NULL)
  expect_data_table(tab, ncol = ncol(reg$status), nrow = 9, key = "job.id")
  #expect_copied(tab, reg$status)

  tab = filter(reg$status, 1:10)
  expect_data_table(tab, ncol = ncol(reg$status), nrow = 9, key = "job.id")
  expect_copied(tab, reg$status)

  tab = filter(reg$status, findJobs(reg = reg))
  expect_data_table(tab, ncol = ncol(reg$status), nrow = 9, key = "job.id")
  expect_copied(tab, reg$status)

  tab = filter(reg$status, data.table(job.id = 3:4, key = "job.id"))
  expect_data_table(tab, ncol = ncol(reg$status), nrow = 1, key = "job.id")

  tab = filter(reg$status, as.data.frame(findJobs(reg = reg)))
  expect_data_table(tab, ncol = ncol(reg$status), key = "job.id")
  expect_copied(tab, reg$status)


  expect_error(filter(reg$status, as.character(1:3)), "not recognized")
})
