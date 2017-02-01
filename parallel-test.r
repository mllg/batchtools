library(batchtools)

fd = "~/parallel-test"
unlink(fd, recursive = TRUE)
reg = makeRegistry(file.dir = fd, packages = "rscimark")

batchMap(function(i) { parallelMap::parallelMap(function(x) rscimark(), 1:5) }, 1)

submitJobs(resources = list(ncpus = 5, memory = 32000, pm.backend = "multicore"))
showLog(1)

waitForJobs()
getJobTable()

reduceResultsList()
showLog(1)

runOSCommand("docker", c("run", "-c 10000", "-m 8g", "arch-r", "/bin/bash"), stop.on.exit.code = FALSE)

reg$cluster.functions$hooks$post.sync
runOSCommand("docker", c("ps", "-a", "--format={{.ID}}", "--filter 'label=batchtools'", "--filter 'status=exited'"))
