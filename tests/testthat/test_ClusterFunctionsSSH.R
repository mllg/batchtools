context("cf ssh")

test_that("cf ssh", {
  skip_on_os("windows")
  skip_on_cran()

  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  if (reg$cluster.functions$name == "Interactive") {
    workers = list(Worker$new("localhost", ncpus = 2, max.load = 9999))
    reg$cluster.functions = makeClusterFunctionsSSH(workers)
    fun = function(x) { Sys.sleep(x); is(x, "numeric") }
    ids = batchMap(fun, x = c(5, 5), reg = reg)
    silent({
      submitJobs(1:2, reg = reg)
      expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
      expect_true(killJobs(2, reg = reg)$killed)
      expect_true(waitForJobs(1, sleep = 0.5, reg = reg))
    })
    expect_equal(findDone(reg = reg), findJobs(ids = 1, reg = reg))
    expect_equal(findNotDone(reg = reg), findJobs(ids = 2, reg = reg))
    expect_true(loadResult(1, reg = reg))
  }
})
