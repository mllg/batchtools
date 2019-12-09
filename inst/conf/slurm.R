## this is used in tests/testthat/test_slurm.R
cluster.functions = makeClusterFunctionsSlurm(system.file(
  file.path("templates", "slurm-simple.tmpl"),
  package="batchtools",
  mustWork=TRUE))
