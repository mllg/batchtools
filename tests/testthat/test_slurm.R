if(interactive())library(testthat)
if(interactive())library(batchtools)
context("slurm")

conf.file <- system.file(
  "conf", "slurm.R", package="batchtools", mustWork=TRUE)
res.list <- list(
  walltime = 3600, #in seconds.
  ncpus=1,
  ntasks=1)
sinResult <- function(reg.name){
  reg.path <- file.path(tempdir(), reg.name)
  unlink(reg.path, recursive=TRUE)
  reg <- makeRegistry(reg.path, conf.file=conf.file)
  batchMap(sin, c(0, pi/2), reg=reg)
  submitJobs(resources=res.list, reg=reg)
  waitForJobs(reg=reg)
  st <- getJobStatus(reg=reg)
  sapply(st$job.id, loadResult)
}
expected <- c(0, 1)

test_that("registry with NO spaces/parens works on slurm", {
  computed <- sinResult("reg")
  expect_equal(computed, expected)
})

test_that("registry WITH spaces/parens works on slurm", {
  computed <- sinResult("reg (!)")
  expect_equal(computed, expected)
})

