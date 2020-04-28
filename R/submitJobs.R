#' @title Submit Jobs to the Batch Systems
#'
#' @description
#' Submits defined jobs to the batch system.
#'
#' After submitting the jobs, you can use \code{\link{waitForJobs}} to wait for the
#' termination of jobs or call \code{\link{reduceResultsList}}/\code{\link{reduceResults}}
#' to collect partial results.
#' The progress can be monitored with \code{\link{getStatus}}.
#'
#' @section Resources:
#' You can pass arbitrary resources to \code{submitJobs()} which then are available in the cluster function template.
#' Some resources' names are standardized and it is good practice to stick to the following nomenclature to avoid confusion:
#' \describe{
#'  \item{walltime:}{Upper time limit in seconds for jobs before they get killed by the scheduler. Can be passed as additional column as part of \code{ids} to set per-job resources.}
#'  \item{memory:}{Memory limit in Mb. If jobs exceed this limit, they are usually killed by the scheduler. Can be passed as additional column as part of \code{ids} to set per-job resources.}
#'  \item{ncpus:}{Number of (physical) CPUs to use on the slave. Can be passed as additional column as part of \code{ids} to set per-job resources.}
#'  \item{omp.threads:}{Number of threads to use via OpenMP. Used to set environment variable \dQuote{OMP_NUM_THREADS}. Can be passed as additional column as part of \code{ids} to set per-job resources.}
#'  \item{pp.size:}{Maximum size of the pointer protection stack, see \code{\link[base]{Memory}}.}
#'  \item{blas.threads:}{Number of threads to use for the BLAS backend. Used to set environment variables \dQuote{MKL_NUM_THREADS} and \dQuote{OPENBLAS_NUM_THREADS}. Can be passed as additional column as part of \code{ids} to set per-job resources.}
#'  \item{measure.memory:}{Enable memory measurement for jobs. Comes with a small runtime overhead.}
#'  \item{chunks.as.arrayjobs:}{Execute chunks as array jobs.}
#'  \item{pm.backend:}{Start a \pkg{parallelMap} backend on the slave.}
#'  \item{foreach.backend:}{Start a \pkg{foreach} backend on the slave.}
#'  \item{clusters:}{Resource used for Slurm to select the set of clusters to run \code{sbatch}/\code{squeue}/\code{scancel} on.}
#' }
#'
#' @section Chunking of Jobs:
#' Multiple jobs can be grouped (chunked) together to be executed sequentially on the batch system as a single batch job.
#' This is especially useful to avoid overburding the scheduler by submitting thousands of jobs simultaneously.
#' To chunk jobs together, job ids must be provided as \code{data.frame} with columns \dQuote{job.id} and \dQuote{chunk} (integer).
#' All jobs with the same chunk number will be executed sequentially inside the same batch job.
#' The utility functions \code{\link{chunk}}, \code{\link{binpack}} and \code{\link{lpt}}
#' can assist in grouping jobs.
#'
#' @section Array Jobs:
#' If your cluster supports array jobs, you can set the resource \code{chunks.as.arrayjobs} to \code{TRUE} in order
#' to execute chunks as job arrays on the cluster.
#' For each chunk of size \code{n}, \pkg{batchtools} creates a \code{\link{JobCollection}} of (possibly heterogeneous) jobs which is
#' submitted to the scheduler as a single array job with \code{n} repetitions.
#' For each repetition, the \code{JobCollection} is first read from the file system, then subsetted to the \code{i}-th job using
#' the environment variable \code{reg$cluster.functions$array.var} (depending on the cluster backend, defined automatically) and finally
#' executed.
#'
#' @section Order of Submission:
#' Jobs are submitted in the order of chunks, i.e. jobs which have chunk number
#' \code{sort(unique(ids$chunk))[1]} first, then jobs with chunk number \code{sort(unique(ids$chunk))[2]}
#' and so on. If no chunks are provided, jobs are submitted in the order of \code{ids$job.id}.
#'
#' @section Limiting the Number of Jobs:
#' If requested, \code{submitJobs} tries to limit the number of concurrent jobs of the user by waiting until jobs terminate
#' before submitting new ones.
#' This can be controlled by setting \dQuote{max.concurrent.jobs} in the configuration file (see \code{\link{Registry}})
#' or by setting the resource \dQuote{max.concurrent.jobs} to the maximum number of jobs to run simultaneously.
#' If both are set, the setting via the resource takes precedence over the setting in the configuration.
#'
#' @section Measuring Memory:
#' Setting the resource \code{measure.memory} to \code{TRUE} turns on memory measurement:
#' \code{\link[base]{gc}} is called  directly before and after the job and the difference is
#' stored in the internal database. Note that this is just a rough estimate and does
#' neither work reliably for external code like C/C++ nor in combination with threading.
#'
#' @section Inner Parallelization:
#' Inner parallelization is typically done via threading, sockets or MPI.
#' Two backends are supported to assist in setting up inner parallelization.
#'
#' The first package is \pkg{parallelMap}.
#' If you set the resource \dQuote{pm.backend} to \dQuote{multicore}, \dQuote{socket} or \dQuote{mpi},
#' \code{\link[parallelMap]{parallelStart}} is called on the slave before the first job in the chunk is started
#' and \code{\link[parallelMap]{parallelStop}} is called after the last job terminated.
#' This way, the resources for inner parallelization can be set and get automatically stored just like other computational resources.
#' The function provided by the user just has to call \code{\link[parallelMap]{parallelMap}} to start parallelization using the preconfigured backend.
#'
#' To control the number of CPUs, you have to set the resource \code{ncpus}.
#' Otherwise \code{ncpus} defaults to the number of available CPUs (as reported by (see \code{\link[parallel]{detectCores}}))
#' on the executing machine for multicore and socket mode and defaults to the return value of \code{\link[Rmpi]{mpi.universe.size}-1} for MPI.
#' Your template must be set up to handle the parallelization, e.g. request the right number of CPUs or start R with \code{mpirun}.
#' You may pass further options like \code{level} to \code{\link[parallelMap]{parallelStart}} via the named list \dQuote{pm.opts}.
#'
#' The second supported parallelization backend is \pkg{foreach}.
#' If you set the resource \dQuote{foreach.backend} to \dQuote{seq} (sequential mode), \dQuote{parallel} (\pkg{doParallel}) or
#' \dQuote{mpi} (\pkg{doMPI}), the requested \pkg{foreach} backend is automatically registered on the slave.
#' Again, the resource \code{ncpus} is used to determine the number of CPUs.
#'
#' Neither the namespace of \pkg{parallelMap} nor the namespace \pkg{foreach} are attached.
#' You have to do this manually via \code{\link[base]{library}} or let the registry load the packages for you.
#'
#' @note
#' If you a large number of jobs, disabling the progress bar (\code{options(batchtools.progress = FALSE)})
#' can significantly increase the performance of \code{submitJobs}.
#'
#' @templateVar ids.default findNotSubmitted
#' @template ids
#' @param resources [\code{named list}]\cr
#'   Computational  resources for the jobs to submit. The actual elements of this list
#'   (e.g. something like \dQuote{walltime} or \dQuote{nodes}) depend on your template file, exceptions are outlined in the section 'Resources'.
#'   Default settings for a system can be set in the configuration file by defining the named list \code{default.resources}.
#'   Note that these settings are merged by name, e.g. merging \code{list(walltime = 300)} into \code{list(walltime = 400, memory = 512)}
#'   will result in \code{list(walltime = 300, memory = 512)}.
#'   Same holds for individual job resources passed as additional column of \code{ids} (c.f. section 'Resources').
#' @param sleep [\code{function(i)} | \code{numeric(1)}]\cr
#'   Parameter to control the duration to sleep between temporary errors.
#'   You can pass an absolute numeric value in seconds or a \code{function(i)} which returns the number of seconds to sleep in the \code{i}-th
#'   iteration between temporary errors.
#'   If not provided (\code{NULL}), tries to read the value (number/function) from the configuration file (stored in \code{reg$sleep}) or defaults to
#'   a function with exponential backoff between 5 and 120 seconds.
#' @template reg
#' @return [\code{\link{data.table}}] with columns \dQuote{job.id} and \dQuote{chunk}.
#' @export
#' @examples
#' \dontshow{ batchtools:::example_push_temp(3) }
#' ### Example 1: Submit subsets of jobs
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # toy function which fails if x is even and an input file does not exists
#' fun = function(x, fn) if (x %% 2 == 0 && !file.exists(fn)) stop("file not found") else x
#'
#' # define jobs via batchMap
#' fn = tempfile()
#' ids = batchMap(fun, 1:20, reg = tmp, fn = fn)
#'
#' # submit some jobs
#' ids = 1:10
#' submitJobs(ids, reg = tmp)
#' waitForJobs(ids, reg = tmp)
#' getStatus(reg = tmp)
#'
#' # create the required file and re-submit failed jobs
#' file.create(fn)
#' submitJobs(findErrors(ids, reg = tmp), reg = tmp)
#' getStatus(reg = tmp)
#'
#' # submit remaining jobs which have not yet been submitted
#' ids = findNotSubmitted(reg = tmp)
#' submitJobs(ids, reg = tmp)
#' getStatus(reg = tmp)
#'
#' # collect results
#' reduceResultsList(reg = tmp)
#'
#' ### Example 2: Using memory measurement
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # Toy function which creates a large matrix and returns the column sums
#' fun = function(n, p) colMeans(matrix(runif(n*p), n, p))
#'
#' # Arguments to fun:
#' args = data.table::CJ(n = c(1e4, 1e5), p = c(10, 50)) # like expand.grid()
#' print(args)
#'
#' # Map function to create jobs
#' ids = batchMap(fun, args = args, reg = tmp)
#'
#' # Set resources: enable memory measurement
#' res = list(measure.memory = TRUE)
#'
#' # Submit jobs using the currently configured cluster functions
#' submitJobs(ids, resources = res, reg = tmp)
#'
#' # Retrive information about memory, combine with parameters
#' info = ijoin(getJobStatus(reg = tmp)[, .(job.id, mem.used)], getJobPars(reg = tmp))
#' print(unwrap(info))
#'
#' # Combine job info with results -> each job is aggregated using mean()
#' unwrap(ijoin(info, reduceResultsDataTable(fun = function(res) list(res = mean(res)), reg = tmp)))
#'
#' ### Example 3: Multicore execution on the slave
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # Function which sleeps 10 seconds, i-times
#' f = function(i) {
#'   parallelMap::parallelMap(Sys.sleep, rep(10, i))
#' }
#'
#' # Create one job with parameter i=4
#' ids = batchMap(f, i = 4, reg = tmp)
#'
#' # Set resources: Use parallelMap in multicore mode with 4 CPUs
#' # batchtools internally loads the namespace of parallelMap and then
#' # calls parallelStart() before the job and parallelStop() right
#' # after the job last job in the chunk terminated.
#' res = list(pm.backend = "multicore", ncpus = 4)
#'
#' \dontrun{
#' # Submit both jobs and wait for them
#' submitJobs(resources = res, reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' # If successfull, the running time should be ~10s
#' getJobTable(reg = tmp)[, .(job.id, time.running)]
#'
#' # There should also be a note in the log:
#' grepLogs(pattern = "parallelMap", reg = tmp)
#' }
submitJobs = function(ids = NULL, resources = list(), sleep = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE)
  assertList(resources, names = "strict")
  resources = insert(reg$default.resources, resources)
  if (hasName(resources, "pm.backend"))
    assertChoice(resources$pm.backend, c("local", "multicore", "socket", "mpi"))
  if (hasName(resources, "foreach.backend"))
    assertChoice(resources$foreach.backend, c("seq", "parallel", "mpi"))
  if (hasName(resources, "pm.opts"))
    assertList(resources$pm.opts, names = "unique")
  if (hasName(resources, "ncpus"))
    assertCount(resources$ncpus, positive = TRUE)
  if (hasName(resources, "measure.memory"))
    assertFlag(resources$measure.memory)
  sleep = getSleepFunction(reg, sleep)

  ids = convertIds(reg, ids, default = .findNotSubmitted(reg = reg), keep.extra = c("chunk", batchtools$resources$per.job))
  if (nrow(ids) == 0L)
    return(noIds())

  # handle chunks
  use.chunking = hasName(ids, "chunk") && anyDuplicated(ids, by = "chunk") > 0L
  if (use.chunking) {
    ids$chunk = asInteger(ids$chunk, any.missing = FALSE)
    chunks = sort(unique(ids$chunk))
  } else {
    chunks = ids$chunk = seq_row(ids)
  }

  # check for jobs already on system
  on.sys = .findOnSystem(reg = reg, cols = c("job.id", "batch.id"))
  ids.on.sys = on.sys[ids, nomatch = 0L, on = "job.id"]
  if (nrow(ids.on.sys) > 0L)
    stopf("Some jobs are already on the system, e.g. %i", ids.on.sys[1L, ]$job.id)

  # handle max.concurrent.jobs
  max.concurrent.jobs = assertCount(resources$max.concurrent.jobs, null.ok = TRUE) %??%
    assertCount(reg$max.concurrent.jobs, null.ok = TRUE) %??% NA_integer_

  # handle chunks.as.arrayjobs
  chunks.as.arrayjobs = FALSE
  if (hasName(resources, "chunks.as.arrayjobs")) {
    assertFlag(resources$chunks.as.arrayjobs)
    if (resources$chunks.as.arrayjobs) {
      if (is.na(reg$cluster.functions$array.var)) {
        info("Ignoring resource 'chunks.as.arrayjobs', not supported by cluster functions '%s'", reg$cluster.functions$name)
      } else {
        chunks.as.arrayjobs = TRUE
      }
    }
  }

  if (!is.na(max.concurrent.jobs)) {
    if (uniqueN(on.sys, by = "batch.id") + (!chunks.as.arrayjobs) * length(chunks) + chunks.as.arrayjobs * nrow(ids) > max.concurrent.jobs) {
      "!DEBUG [submitJobs]: Limiting the number of concurrent jobs to `max.concurrent.jobs`"
    } else {
      max.concurrent.jobs = NA_integer_
    }
  }

  # handle job resources
  per.job.resources = chintersect(names(ids), batchtools$resources$per.job)
  if (length(per.job.resources) > 0L) {
    if (use.chunking)
      stopf("Combining per-job resources with chunking is not supported")
    ids$resource.id = addResources(reg, .mapply(function(...) insert(resources, list(...)), ids[, per.job.resources, with = FALSE], MoreArgs = list()))
    ids[, (per.job.resources) := NULL]
  } else {
    ids$resource.id = addResources(reg, list(resources))
  }

  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)
  on.exit(saveRegistry(reg))

  chunk = NULL
  runHook(reg, "pre.submit")

  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta")
  pb$tick(0, tokens = list(status = "Submitting"))

  for (ch in chunks) {
    ids.chunk = ids[chunk == ch, c("job.id", "resource.id")]
    jc = makeJobCollection(ids.chunk, resources = reg$resources[ids.chunk, on = "resource.id"]$resources[[1L]], reg = reg)
    if (reg$cluster.functions$store.job.collection)
      writeRDS(jc, file = jc$uri, compress = jc$compress)

    # do we have to wait for jobs to get terminated before proceeding?
    if (!is.na(max.concurrent.jobs)) {
      # count chunks or job.id
      i = 1L
      repeat {
        n.on.sys = uniqueN(getBatchIds(reg), by = "batch.id")
        "!DEBUG [submitJobs]: Detected `n.on.sys` batch jobs on system (`max.concurrent.jobs` allowed concurrently)"

        if (n.on.sys < max.concurrent.jobs)
          break
        pb$tick(0, tokens = list(status = "Waiting   "))
        sleep(i)
        i = i + 1L
      }
    }

    # remove old result files
    fns = getResultFiles(reg, ids.chunk)
    file_remove(fns)

    i = 1L
    repeat {
      runHook(reg, "pre.submit.job")
      now = ustamp()
      submit = reg$cluster.functions$submitJob(reg = reg, jc = jc)

      if (submit$status == 0L) {
        if (!testCharacter(submit$batch.id, any.missing = FALSE, min.len = 1L)) {
          stopf("Cluster function did not return valid batch ids:\n%s", stri_flatten(capture.output(str(submit$batch.id)), "\n"))
        }
        reg$status[ids.chunk,
          c("submitted", "started", "done",   "error",       "mem.used", "resource.id",         "batch.id",      "log.file",      "job.hash") :=
          list(now,      NA_real_,  NA_real_, NA_character_, NA_real_,   ids.chunk$resource.id, submit$batch.id, submit$log.file, jc$job.hash)]
        runHook(reg, "post.submit.job")
        break
      } else if (submit$status > 0L && submit$status < 100L) {
        # temp error
        pb$tick(0, tokens = list(status = submit$msg))
        sleep(i)
        i = i + 1L
      } else if (submit$status > 100L && submit$status <= 200L) {
        # fatal error
        stopf("Fatal error occurred: %i. %s", submit$status, submit$msg)
      }
    }
    pb$tick(len = 1, tokens = list(status = "Submitting"))
  }

  Sys.sleep(reg$cluster.functions$scheduler.latency)
  runHook(reg, "post.submit")

  # return ids, registry is saved via on.exit()
  return(invisible(ids))
}

addResources = function(reg, resources) {
  ai = function(tab, col) { # auto increment by reference
    i = tab[is.na(get(col)), which = TRUE]
    if (length(i) > 0L) {
      ids = seq_along(i)
      if (length(i) < nrow(tab))
        ids = ids + max(tab[, max(col, na.rm = TRUE), with = FALSE][[1L]], na.rm = TRUE)
      tab[i, (col) := ids]
      setkeyv(tab, col)[]
    }
  }

  tab = data.table(resources = resources, resource.hash = vcapply(resources, digest))
  new.tab = unique(tab, by = "resource.hash")[!reg$resources, on = "resource.hash"]
  if (nrow(new.tab)) {
    reg$resources = rbindlist(list(reg$resources, new.tab), fill = TRUE, use.names = TRUE)
    ai(reg$resources, "resource.id")
  }
  reg$resources[tab, "resource.id", on = "resource.hash"][[1L]]
}
