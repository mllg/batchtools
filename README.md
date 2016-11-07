# batchtools

[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/batchtools)](https://cran.r-project.org/package=batchtools)
[![Build Status](https://travis-ci.org/mllg/batchtools.svg?branch=master)](https://travis-ci.org/mllg/batchtools)
[![Build status](https://ci.appveyor.com/api/projects/status/ypp14tiiqfhnv92k?svg=true)](https://ci.appveyor.com/project/mllg/batchtools/branch/master)
[![Coverage Status](https://img.shields.io/coveralls/mllg/batchtools.svg)](https://coveralls.io/r/mllg/batchtools?branch=master)

As a successor of the packages [BatchJobs](https://github.com/tudo-r/BatchJobs) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments), batchtools provides a parallel implementation of Map for high performance computing systems managed by schedulers like Slurm, Torque, or SGE.
Moreover, the package provides an abstraction mechanism to define large-scale computer experiments in a well-organized and reproducible way.

## Installation
Install the development version via [devtools](https://cran.r-project.org/package=devtools):
```{R}
devtools::install_github("mllg/batchtools")
```

## Resources
* [NEWS](https://github.com/mllg/batchtools/blob/master/NEWS.md) including a comparison with [BatchJobs](https://github.com/tudo-r/BatchJobs) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments)
* [Setup](https://mllg.github.io/batchtools/articles/Setup)
* [Documentation and Vignettes](https://mllg.github.io/batchtools/)
* [Paper on BatchJobs/BatchExperiments](http://www.jstatsoft.org/v64/i11)
