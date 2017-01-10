context("mergeRegistries")

test_that("mergeRegistries", {
  target = makeRegistry(NA, make.default = FALSE)
  f = function(.job, x) { if (x %in% c(2, 7)) file.create(file.path(.job$external.dir, "foo")); x^2 }
  batchMap(f, 1:10, reg = target)

  td = file.path(target$temp.dir, basename(tempfile()))
  dir.create(td)
  file.copy(target$file.dir, td, recursive = TRUE)
  file.dir = file.path(td, basename(target$file.dir))
  source = loadRegistry(file.dir, update.paths = TRUE)

  submitAndWait(target, data.table(job.id = 1:4, chunk = 1L))
  submitAndWait(source, data.table(job.id = 6:9, chunk = c(1L, 1L, 1L, 2L)))
  expect_data_table(findDone(reg = source), nrow = 4)
  expect_data_table(findDone(reg = target), nrow = 4)

  mergeRegistries(source, target)
  expect_data_table(findDone(reg = source), nrow = 4)
  expect_data_table(findDone(reg = target), nrow = 8)

  checkTables(target)

  expect_set_equal(list.files(getExternalPath(target)), as.character(c(2, 7)))
  expect_equal(reduceResultsDataTable(reg = target)$V1, c(1,2,3,4,6,7,8,9)^2)
  expect_file_exists(file.path(target$file.dir, "external", c("2", "7"), "foo"))
})
