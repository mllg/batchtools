cluster.functions = makeClusterFunctionsSlurm(system.file(
  file.path("templates", "slurm-simple.tmpl"),
  package="batchtools",
  mustWork=TRUE))
