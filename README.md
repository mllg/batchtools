# batchtools

[![JOSS Publicatoin](https://joss.theoj.org/papers/10.21105/joss.00135/status.svg)](https://doi.org/10.21105/joss.00135)
[![CRAN Status Badge](https://www.r-pkg.org/badges/version/batchtools)](https://cran.r-project.org/package=batchtools)
[![Build Status](https://travis-ci.org/mllg/batchtools.svg?branch=master)](https://travis-ci.org/mllg/batchtools)
[![Build Status](https://ci.appveyor.com/api/projects/status/ypp14tiiqfhnv92k/branch/master?svg=true)](https://ci.appveyor.com/project/mllg/batchtools/branch/master)

As a successor of the packages [BatchJobs](https://github.com/tudo-r/BatchJobs) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments), batchtools provides a parallel implementation of Map for high performance computing systems managed by schedulers like Slurm, Sun Grid Engine, OpenLava, TORQUE/OpenPBS, Load Sharing Facility (LSF) or Docker Swarm (see the setup section in the [vignette](https://mllg.github.io/batchtools/articles/batchtools.html)).

Main features:
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

Next, you need to setup `batchtools` for your HPC (it will run sequentially otherwise).
See the [vignette](https://mllg.github.io/batchtools/articles/batchtools.html#setup) for instructions.

## Why batchtools?
The development of [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) is discontinued for the following reasons:

* Maintainability: The packages [BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) are tightly connected which makes maintenance difficult. Changes have to be synchronized and tested against the current CRAN versions for compatibility. Furthermore, BatchExperiments violates CRAN policies by calling internal functions of BatchJobs.
* Data base issues: Although we invested weeks to mitigate issues with locks of the SQLite data base or file system (staged queries, file system timeouts, ...), `BatchJobs` kept working unreliable on some systems with high latency under certain conditions. This made `BatchJobs` unusable for many users.

[BatchJobs](https://github.com/tudo-r/BatchJobs/) and [BatchExperiments](https://github.com/tudo-r/Batchexperiments) will remain on CRAN, but new features are unlikely to be ported back.
The [vignette](https://mllg.github.io/batchtools/articles/batchtools.html#migration) contains a section comparing the packages.


## Resources
* [NEWS](https://mllg.github.io/batchtools/news/)
* [Function reference](https://mllg.github.io/batchtools/reference/)
* [Vignette](https://mllg.github.io/batchtools/articles/batchtools.html)
* [JOSS Paper](https://doi.org/10.21105/joss.00135): Short paper on batchtools. Please cite this if you use batchtools.
* [Paper on BatchJobs/BatchExperiments](https://www.jstatsoft.org/v64/i11): The described concept still holds for batchtools and most examples work analogously (see the [vignette](https://mllg.github.io/batchtools/articles/batchtools.html#migration) for differences between the packages).

## Citation
Please cite the [JOSS paper](https://doi.org/10.21105/joss.00135) using the following BibTeX entry:
```
@article{,
  doi = {10.21105/joss.00135},
  url = {https://doi.org/10.21105/joss.00135},
  year  = {2017},
  month = {feb},
  publisher = {The Open Journal},
  volume = {2},
  number = {10},
  author = {Michel Lang and Bernd Bischl and Dirk Surmann},
  title = {batchtools: Tools for R to work on batch systems},
  journal = {The Journal of Open Source Software}
}
```

## Related Software
* The [High Performance Computing Task View](https://cran.r-project.org/view=HighPerformanceComputing) lists the most relevant packages for scientific computing with R.
* [clustermq](https://cran.r-project.org/package=clustermq) is a similar approach which also supports multiple schedulers. Uses the ZeroMQ network protocol for communication, and shines if you have millions of fast jobs.
* [batch](https://cran.r-project.org/package=batch) assists in splitting and submitting jobs to LSF and MOSIX clusters.
* [flowr](https://cran.r-project.org/package=flowr) supports LSF, Slurm, TORQUE and Moab and provides a scatter-gather approach to define computational jobs.
* [future.batchtools](https://cran.r-project.org/package=future.batchtools) implements `batchtools` as backend for [future](https://cran.r-project.org/package=future.batchtools).
* [doFuture](https://cran.r-project.org/package=doFuture) together with [future.batchtools](https://cran.r-project.org/package=future.batchtools) connects `batchtools` to [foreach](https://cran.r-project.org/package=foreach).
* [drake](https://cran.r-project.org/package=drake) uses graphs to define computational jobs. `batchtools` is used as a backend via [future.batchtools](https://cran.r-project.org/package=future.batchtools).

## Contributing to batchtools
This R package is licensed under the [LGPL-3](https://www.gnu.org/licenses/lgpl-3.0.en.html).
If you encounter problems using this software (lack of documentation, misleading or wrong documentation, unexpected behaviour, bugs, ...) or just want to suggest features, please open an issue in the [issue tracker](https://github.com/mllg/batchtools/issues).
Pull requests are welcome and will be included at the discretion of the author.
If you have customized a template file for your (larger) computing site, please share it: fork the repository, place your template in `inst/templates` and send a pull request.
