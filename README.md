# batchtools

[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/batchtools)](https://cran.r-project.org/package=batchtools)
[![Build Status](https://travis-ci.org/mllg/batchtools.svg?branch=master)](https://travis-ci.org/mllg/batchtools)
[![Build status](https://ci.appveyor.com/api/projects/status/1gdgk7twxrghi943/branch/master?svg=true)](https://ci.appveyor.com/project/mllg/batchtools-jgbhb/branch/master)
[![Coverage Status](https://img.shields.io/coveralls/mllg/batchtools.svg)](https://coveralls.io/r/mllg/batchtools?branch=master)

As a successor of the packages [BatchJobs](https://github.com/tudo-r/BatchJobs) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments), batchtools provides a parallel implementation of Map for high performance computing systems managed by schedulers like Slurm, Sun Grid Engine, OpenLava, Torque/OpenPBS, Load Sharing Facility (LSF) or DockerSwarm (see the [Setup vignette](https://mllg.github.io/batchtools/articles/Setup)).

The main features conclude:
* Convenience: All relevant batch system operations (submitting, listing, killing) are either handled internally or abstracted via simple R functions
* Portability: A well-defined interface makes the package applicable in most high-performance computing environments
* Reproducibility: Every computational part has an associated seed stored in a data base which ensures reproducibility even when the underlying batch system changes
* Abstraction: The code layers for algorithms, experiment definitions and execution are cleanly separated and allow to write readable and maintainable code to manage even large scale computer experiments


## Installation
Install the stable version from CRAN:
```{R}
install.packages("batchtools")
```
For the development version, use [devtools](https://cran.r-project.org/package=devtools):
```{R}
devtools::install_github("mllg/batchtools")
```

## Why batchtools?
The development of [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) is discontinued because of the following reasons:

* Maintainability: The packages [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) are tightly connected which makes maintaining difficult. Changes have to be synchronized and tested against the current CRAN versions for compatibility. Furthermore, BatchExperiments violates CRAN policies by calling internal functions of BatchJobs.
* Data base issues: Although we invested weeks to mitigate issues with locks of the SQLite data base or file system (staged queries, file system timeouts, ...), `BatchJobs` kept working unreliable on some systems with high latency or specific file systems. This made `BatchJobs` unusable for many users.

[BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) will remain on CRAN, but new features are unlikely to be ported back.
Changes are covered in the [NEWS](https://github.com/mllg/batchtools/blob/master/NEWS.md).

## Resources
* [NEWS](https://github.com/mllg/batchtools/blob/master/NEWS.md)
* [Setup](https://mllg.github.io/batchtools/articles/Setup)
* [Documentation and Vignettes](https://mllg.github.io/batchtools/)
* [Paper on BatchJobs/BatchExperiments](http://www.jstatsoft.org/v64/i11)


## Related Software
* The [High Performance Computing Task View](https://cran.r-project.org/web/views/HighPerformanceComputing.html) lists the most relevant packages for scientific computing with R
* [batch](https://cran.r-project.org/package=batch) assists in splitting and submitting jobs to LSF and MOSIX clusters
* [flowr](https://cran.r-project.org/package=flowr) supports LSF, Slurm, Torque and Moab and provides a scatter-gather approach to define computational jobs
