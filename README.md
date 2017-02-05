# batchtools

[![CRAN Status Badge](http://www.r-pkg.org/badges/version/batchtools)](https://cran.r-project.org/package=batchtools)
[![Build Status](https://travis-ci.org/mllg/batchtools.svg?branch=master)](https://travis-ci.org/mllg/batchtools)
[![Build Status](https://ci.appveyor.com/api/projects/status/ypp14tiiqfhnv92k/branch/master?svg=true)](https://ci.appveyor.com/project/mllg/batchtools/branch/master)
[![Coverage Status](https://img.shields.io/coveralls/mllg/batchtools.svg)](https://coveralls.io/r/mllg/batchtools?branch=master)

As a successor of the packages [BatchJobs](https://github.com/tudo-r/BatchJobs) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments), batchtools provides a parallel implementation of Map for high performance computing systems managed by schedulers like Slurm, Sun Grid Engine, OpenLava, TORQUE/OpenPBS, Load Sharing Facility (LSF) or Docker Swarm (see the [Setup vignette](https://mllg.github.io/batchtools/articles/v00_Setup)).

The main features conclude:
* Convenience: All relevant batch system operations (submitting, listing, killing) are either handled internally or abstracted via simple R functions
* Portability: With a well-defined interface, the source is independent from the underlying batch system - prototype locally, deploy on any high performance cluster
* Reproducibility: Every computational part has an associated seed stored in a data base which ensures reproducibility even when the underlying batch system changes
* Abstraction: The code layers for algorithms, experiment definitions and execution are cleanly separated and allow to write readable and maintainable code to manage large scale computer experiments


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
The development of [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) is discontinued for the following reasons:

* Maintainability: The packages [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) are tightly connected which makes maintenance difficult. Changes have to be synchronized and tested against the current CRAN versions for compatibility. Furthermore, BatchExperiments violates CRAN policies by calling internal functions of BatchJobs.
* Data base issues: Although we invested weeks to mitigate issues with locks of the SQLite data base or file system (staged queries, file system timeouts, ...), `BatchJobs` kept working unreliable on some systems with high latency or specific file systems. This made `BatchJobs` unusable for many users.

[BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) will remain on CRAN, but new features are unlikely to be ported back.
See this [vignette](https://mllg.github.io/batchtools/articles/v01_Migration) for a comparison of the packages.


## Resources
* [NEWS](https://mllg.github.io/batchtools/news/)
* [Function reference](https://mllg.github.io/batchtools/reference)
* [Vignettes](https://mllg.github.io/batchtools/articles)
* [Paper on BatchJobs/BatchExperiments](http://www.jstatsoft.org/v64/i11): The described concept still holds for batchtools and most examples work analogously (see this [vignette](https://mllg.github.io/batchtools/articles/v01_Migration) for differences between the packages).


## Related Software
* The [High Performance Computing Task View](https://cran.r-project.org/web/views/HighPerformanceComputing.html) lists the most relevant packages for scientific computing with R
* [batch](https://cran.r-project.org/package=batch) assists in splitting and submitting jobs to LSF and MOSIX clusters
* [flowr](https://cran.r-project.org/package=flowr) supports LSF, Slurm, TORQUE and Moab and provides a scatter-gather approach to define computational jobs

## Contributing to batchtools
This R package is licensed under the [LGPL-3](https://www.gnu.org/licenses/lgpl-3.0.en.html).
If you encounter problems using this software (lack of documentation, misleading or wrong documentation, unexpected behaviour, bugs, ...) or just want to suggest features, please open an issue in the [issue tracker](https://github.com/mllg/batchtools/issues).
Pull requests are welcome and will be included at the discretion of the author.
If you have customized a template file for your (larger) computing site, please share it: fork the repository, place your template in `inst/templates` and send a pull request.
