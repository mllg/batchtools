context("parallelMap")

silent({
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  fun = function(i) { fun = function(i) i^2; parallelMap::parallelMap(fun, 1:i)}
  ids = batchMap(fun, i = 1:4, reg = reg)
})

test_that("pm/multicore", {
  skip_on_os("windows")
  skip_if_not_installed("parallelMap")
  skip_on_travis()
  if (reg$cluster.functions$name %chin% c("Parallel", "Socket"))
    skip("Nested local parallelization not supported")

  submitAndWait(reg, ids = ids, resources = list(pm.backend = "multicore", ncpus = 2))
  expect_equal(nrow(findDone(reg = reg)), 4L)
})

test_that("pm/socket", {
  skip_if_not_installed("parallelMap")
  skip_if_not_installed("snow")
  skip_on_travis()
  if (reg$cluster.functions$name %chin% c("Parallel", "Socket"))
    skip("Nested local parallelization not supported")

  submitAndWait(reg, ids = ids, resources = list(pm.backend = "socket", ncpus = 2))
  expect_equal(nrow(findDone(reg = reg)), 4L)
})

# test_that("pm/mpi", {
#   skip_on_os("mac")
#   skip_on_cran()
#   skip_if_not_installed("parallelMap")
#   skip_if_not_installed("snow")
#   skip_if_not_installed("Rmpi")
#   skip_on_travis()
#   if (reg$cluster.functions$name %chin% c("Parallel", "Socket"))
#     skip("Nested local parallelization not supported")

#   submitAndWait(reg, ids = ids, resources = list(pm.backend = "mpi", ncpus = 2))
#   expect_equal(nrow(findDone(reg = reg)), 4)
# })
