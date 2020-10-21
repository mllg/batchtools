---
title: 'batchtools: Tools for R to work on batch systems'
tags:
- R
 - high-performance computing
 - batch systems
 - parallelization
authors:
 - name: Michel Lang
   orcid: 0000-0001-9754-0393
   affiliation: 1
 - name: Bernd Bischl
   orcid: 0000-0001-6002-6980
   affiliation: 2
 - name: Dirk Surmann
   orcid: 0000-0003-0873-137X
   affiliation: 1
affiliations:
 - name: TU Dortmund University
   index: 1
 - name: LMU Munich
   index: 2
date: 9 November 2016
bibliography: paper.bib
---

# Summary

The [`R`](https://www.r-project.org/) [@R] package [`batchtools`](https://github.com/mllg/batchtools) is the successor of the [`BatchJobs`](https://github.com/tudo-r/BatchJobs) package [@batchjobs_2015].
It provides an implementation of a Map-like operation to define and asynchronously execute jobs on a variety of parallel backends:

* Local (blocking) execution in the current `R` session or in an externally spawned `R` process (intended for debugging and prototyping)
* Local (non-blocking) parallel execution using `parallel`'s multicore backend [@R] or [`snow`](https://cran.r-project.org/package=snow)'s socket mode [@snow].
* Execution on loosely connected machines using SSH (including basic resource usage control).
* [Docker Swarm](https://docs.docker.com/engine/swarm/)
* [IBM Spectrum LSF](https://www.ibm.com/products/hpc-workload-management)
* [OpenLava](https://www.openlava.org/)
* [Univa Grid Engine](https://www.univa.com/) (formerly Oracle Grind Engine and Sun Grid Engine)
* [Slurm Workload Manager](https://slurm.schedmd.com/)
* [TORQUE/PBS Resource Manager](https://adaptivecomputing.com/cherry-services/moab-hpc/)

Extensibility and user customization are important features as configuration on high-performance computing clusters is often heavily tailored towards very specific requirements or special hardware.
Hence, the interaction with the schedulers uses a template engine for improved flexibility.
Furthermore, custom functions can be hooked into the package to be called at certain events.
As a last resort, many utility functions simplify the implementation of a custom cluster backend from scratch.

The communication between the master `R` session and the computational nodes is kept as simple as possible and runs completely on the file system which greatly simplifies the extension to additional parallel platforms.
The [`data.table`](https://github.com/Rdatatable/data.table) package [@data_table] acts as an in-memory database to keep track of the computational status of all jobs.
Unique job seeds ensure reproducibility across systems, log files can conveniently be searched using regular expressions and jobs can be annotated with arbitrary tags.
Jobs can be chunked (i.e., merged into one technical cluster job) to be executed as one virtual job on a node (executed sequentially or using multiple local CPUs) in order to reduce the overhead induced by job management and starting/stopping `R`.
All in all, the provided tools allow users to work with many thousands or even millions of jobs in an organized and efficient manner.

The `batchtools` package also comes with an abstraction mechanism to assist in conducting large-scale computer experiments, especially suited for (but not restricted to) benchmarking and exploration of algorithm performance.
The mechanism is similar to [`BatchExperiments`](https://github.com/tudo-r/BatchExperiments) [@batchjobs_2015] which `batchtools` now also supersedes:
After defining the building blocks of most computer experiments, problems and algorithms, both can be parametrized to define jobs which are then in a second step submitted to one of the parallel backends.

Important changes to its predecessors are summarized in a vignette to help users of [`BatchJobs`](https://github.com/tudo-r/BatchJobs)/[`BatchExperiments`](https://github.com/tudo-r/BatchExperiments) migrating their cluster configuration and aid the transition to `batchtools`.


# References

