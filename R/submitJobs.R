#' @title Submit Jobs to the Batch Systems
#'
#' @description
#' Submits defined jobs to the batch system.
#'
#' If an additional column \dQuote{chunk} is found in the table \code{ids},
#' jobs will be grouped accordingly to be executed sequentially on the same slave.
#' The utility functions \code{\link{chunk}}, \code{\link{binpack}} and \code{\link{lpt}}
#' can assist in grouping jobs.
#' Jobs are submitted in the order of chunks, i.e. jobs which have chunk number
#' \code{unique(ids$chunk)[1]} first, then jobs with chunk number \code{unique(ids$chunk)[2]}
#' and so on. If no chunks are provided, jobs are submitted in the order of \code{ids$job.id}.
#'
#' After submitting the jobs, you can use \code{\link{waitForJobs}} to wait for the
#' termination of jobs or call \code{\link{reduceResultsList}}/\code{\link{reduceResults}}
#' to collect partial results.
#' The progress can be monitored with \code{\link{getStatus}}.
#'
#' @note
#' Setting the resource \code{measure.memory} to \code{TRUE} turns on memory measurement:
#' \code{\link[base]{gc}} is called  directly before
#' and after the job and the difference is stored in the internal database. Note that this is just a rough estimate and does
#' neither work reliably for external code like C/C++ nor in combination with threading.
#'
#' If your cluster supports array jobs, you can set the resource \code{chunks.as.arrayjobs} to \code{TRUE} in order
#' to execute chunks as job arrays. To do so, the job must be repeated \code{nrow(jobs)} times via the cluster functions template.
#' The function \code{\link{doJobCollection}} (which is called on the slave) now retrieves the repetition number from the environment
#' and restricts the computation to the respective job in the \code{\link{JobCollection}}.
#'
#' Furthermore, the package provides support for inner parallelization using threading, sockets or MPI via the
#' package \pkg{parallelMap}.
#' If you set the resource \dQuote{pm.backend} to \dQuote{multicore}, \dQuote{socket} or \dQuote{mpi},
#' \code{\link[parallelMap]{parallelStart}} is called on the slave before the first job in the chunk is started
#' and \code{\link[parallelMap]{parallelStop}} is called after the last job terminated.
#' This way, the used resources for inner parallelization are set in the same place as the resources for the outer parallelization done by
#' \pkg{batchtools} and all resources get stored together in the \code{\link{Registry}}.
#' The user function just has to call \code{\link[parallelMap]{parallelMap}} to start parallelization using the preconfigured backend.
#'
#' Note that you should set the resource \code{ncpus} to control the number of CPUs to use in \pkg{parallelMap}.
#' \code{ncpus} defaults to the number of available CPUs (as reported by (see \code{\link[parallel]{detectCores}}))
#' on the executing machine for multicore and socket mode and defaults to the return value of \code{\link[Rmpi]{mpi.universe.size}-1} for MPI.
#' Your template must be set up to handle the parallelization, e.g. start R with \code{mpirun} or request the right number of CPUs.
#' You may pass further options like \code{level} to \code{\link[parallelMap]{parallelStart}} via the named list \dQuote{pm.opts}.
#'
#' Also note that if you have thousands of jobs, disabling the progress bar (\code{options(batchtools.progress = FALSE)})
#' can significantly increase the performance of \code{submitJobs}.
#'
#' @templateVar ids.default findNotSubmitted
#' @template ids
#' @param resources [\code{named list}]\cr
#'   Computational  resources for the batch jobs. The elements of this list
#'   (e.g. something like \dQuote{walltime} or \dQuote{nodes}) depend on your template file.
#'   See notes for reserved special resource names.
#'   Defaults can be stored in the configuration file by providing the named list \code{default.resources}.
#'   Settings in \code{resources} overwrite those in \code{default.resources}.
#' @param sleep [\code{function(i)} | \code{numeric(1)}]\cr
#'   Function which returns the duration to sleep in the \code{i}-th iteration between temporary errors.
#'   Alternatively, you can pass a single positive numeric value.
#' @template reg
#' @return [\code{\link{data.table}}] with columns \dQuote{job.id} and \dQuote{chunk}.
#' @export
#' @examples
#' ### Example 1: Using memory measurement
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # Toy function which creates a large matrix and returns the column sums
#' fun = function(n, p) colMeans(matrix(runif(n*p), n, p))
#'
#' # Arguments to fun:
#' args = CJ(n = c(1e4, 1e5), p = c(10, 50)) # like expand.grid()
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
#' info = ijoin(getJobStatus(reg = tmp)[, .(job.id, memory)], getJobPars(reg = tmp))
#' print(info)
#'
#' # Combine job info with results -> each job is aggregated using mean()
#' ijoin(info, reduceResultsDataTable(fun = function(res) list(res = mean(res)), reg = tmp))
#'
#' ### Example 2: Multicore execution on the slave
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
submitJobs = function(ids = NULL, resources = list(), sleep = default.sleep, reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE)
  assertList(resources, names = "strict")
  resources = insert(reg$default.resources, resources)
  if (hasName(resources, "pm.backend"))
    assertChoice(resources$pm.backend, c("local", "multicore", "socket", "mpi"))
  if (hasName(resources, "pm.opts"))
    assertList(resources$pm.opts, names = "unique")
  if (hasName(resources, "ncpus"))
    assertCount(resources$ncpus, positive = TRUE)
  if (hasName(resources, "measure.memory"))
    assertFlag(resources$measure.memory)
  if (hasName(resources, "chunks.as.arrayjobs")) {
    assertFlag(resources$chunks.as.arrayjobs)
    if (resources$chunks.as.arrayjobs && is.na(reg$cluster.functions$array.var)) {
      info("Ignoring resource 'chunks.as.arrayjobs', not supported by cluster functions '%s'", reg$cluster.functions$name)
      resources$chunks.as.arrayjobs = NULL
    }
  }
  sleep = getSleepFunction(sleep)

  ids = convertIds(reg, ids, default = .findNotSubmitted(reg = reg), keep.extra = "chunk")
  if (nrow(ids) == 0L)
    return(noIds())

  # handle chunks
  if (hasName(ids, "chunk")) {
    ids$chunk = asInteger(ids$chunk, any.missing = FALSE)
    chunks = unique(ids$chunk)
  } else {
    chunks = ids$chunk = seq_row(ids)
  }

  # check for jobs already on system
  on.sys = .findOnSystem(reg = reg, cols = c("job.id", "batch.id"))
  ids.on.sys = on.sys[ids, nomatch = 0L, on = "job.id"]
  if (nrow(ids.on.sys) > 0L)
    stopf("Some jobs are already on the system, e.g. %i", ids.on.sys[1L, ]$job.id)

  # handle max.concurrent.jobs
  max.concurrent.jobs = NA_integer_
  if (hasName(reg, "max.concurrent.jobs")) {
    assertCount(reg$max.concurrent.jobs)
    if (uniqueN(on.sys, by = "batch.id") + length(chunks) > reg$max.concurrent.jobs) {
      "!DEBUG [submitJobs]: Limiting the number of concurrent jobs to `reg$max.concurrent.jobs`"
      max.concurrent.jobs = reg$max.concurrent.jobs
    }
  }

  # handle resources
  res.hash = digest(resources)
  resource.hash = NULL
  res.id = reg$resources[resource.hash == res.hash, "resource.id"]$resource.id
  if (length(res.id) == 0L) {
    res.id = auto_increment(reg$resources$resource.id)
    reg$resources = rbind(reg$resources, data.table(resource.id = res.id, resource.hash = res.hash, resources = list(resources)))
    setkeyv(reg$resources, "resource.id")
  }
  on.exit(saveRegistry(reg))

  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)

  chunk = NULL
  runHook(reg, "pre.submit")

  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta")
  pb$tick(0, tokens = list(status = "Submitting"))

  for (ch in chunks) {
    ids.chunk = ids[chunk == ch, "job.id"]
    jc = makeJobCollection(ids.chunk, resources = resources, reg = reg)
    if (reg$cluster.functions$store.job)
      writeRDS(jc, file = jc$uri)

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
    file.remove.safely(fns)

    i = 1L
    repeat {
      runHook(reg, "pre.submit.job")
      now = ustamp()
      submit = reg$cluster.functions$submitJob(reg = reg, jc = jc)

      if (submit$status == 0L) {
        reg$status[ids.chunk,
          c("submitted", "started", "done",   "error",       "memory", "resource.id", "batch.id",      "log.file",      "job.hash") :=
          list(now,      NA_real_,  NA_real_, NA_character_, NA_real_, res.id,        submit$batch.id, submit$log.file, jc$job.hash)]
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
