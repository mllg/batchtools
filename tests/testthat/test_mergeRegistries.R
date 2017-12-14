context("mergeRegistries")

test_that("mergeRegistries", {
  target = makeTestRegistry()
  f = function(.job, x) { if (x %in% c(2, 7)) file.create(fp(.job$external.dir, "foo")); x^2 }
  batchMap(f, 1:10, reg = target)

  td = fp(target$temp.dir, basename(tempfile()))
  dir.create(td)
  file.copy(target$file.dir, td, recursive = TRUE)
  file.dir = fp(td, basename(target$file.dir))
  source = loadRegistry(file.dir, writeable = TRUE)

  submitAndWait(target, data.table(job.id = 1:4, chunk = 1L))
  submitAndWait(source, data.table(job.id = 6:9, chunk = c(1L, 1L, 1L, 2L)))
  expect_data_table(findDone(reg = source), nrow = 4)
  expect_data_table(findDone(reg = target), nrow = 4)

  mergeRegistries(source, target)
  expect_data_table(findDone(reg = source), nrow = 4)
  expect_data_table(findDone(reg = target), nrow = 8)

  checkTables(target)

  expect_set_equal(list.files(dir(target, "external")), as.character(c(2, 7)))
  expect_equal(unwrap(reduceResultsDataTable(reg = target))$result.1, c(1,2,3,4,6,7,8,9)^2)
  expect_file_exists(fp(target$file.dir, "external", c("2", "7"), "foo"))

  unlink(td, recursive = TRUE)
})
