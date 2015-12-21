# batchtools

[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/batchtools)](http://cran.r-project.org/package=batchtools)
[![Build Status](https://travis-ci.org/mllg/batchtools.svg)](https://travis-ci.org/mllg/batchtools)
[![Build status](https://ci.appveyor.com/api/projects/status/ypp14tiiqfhnv92k?svg=true)](https://ci.appveyor.com/project/mllg/batchtools/branch/master)
[![Coverage Status](https://img.shields.io/coveralls/mllg/batchtools.svg)](https://coveralls.io/r/mllg/batchtools?branch=master)

As a successor of the packages BatchJobs and BatchExperiments, batchtools provides a parallel implementation of Map for high performance computing systems managed by schedulers like SLURM, Torque, or SGE.
Moreover, the package provides an abstraction mechanism to define large-scale computer experiments in a well-organized and reproducible way.

The development is still in alpha.

You can browse the documentation and vignettes [here](https://mllg.github.io/batchtools/).

## Configuration

If no configuration is provided, `batchtools` runs in an interactive (sequential) mode.
You can change this by modifying the registry:
```{r}
library(batchtools)

# create an interactive registry
reg = makeRegistry(file.dir = "test")

# switch to SLURM cluster functions
reg$cluster.functions = makeClusterFunctionsSLURM("~/slurm.tmpl")

# set default resources for this systems
reg$default.resources = list(walltime = 60 * 60, memory = 1024)

# make these choices permanent for this registry
saveRegistry(reg)
```
Instead of calling the constructor for the `ClusterFunctions` yourself in every session, you can also source a configuration file.
To do so, create the file `~/.batchtools.conf.r` where you set everything accordingly:
```{r}
cluster.functions = makeClusterFunctionsSLURM("~/slurm.tmpl")
default.resources = list(walltime = 60 * 60, memory = 1024)
```
This file is automatically sourced whenever you create a new registry:
```{r}
reg = makeRegistry(file.dir = "test")
```
The default location of the configuration file can also be set via the option `batchtools.conf.file`.
