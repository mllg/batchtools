#' @title Submit Jobs to the Batch Systems
#'
#' @description
#' Submits all jobs defined with \code{\link{batchMap}} to the batch system.
#'
#' If an additional column \dQuote{chunk} is present in the table \code{ids},
#' the jobs will be grouped accordingly. See \code{\link{chunkIds}} for more
#' information.
#'
#' After submitting the jobs, you can use \code{\link{waitForJobs}} to wait for the
#' termination of jobs or immediately call \code{\link{reduceResultsList}}/\code{\link{reduceResults}}
#' to collect partial results.
#' The progress can be monitored with \code{\link{getJobStatus}}.
#'
#' @note
#' Setting the resource \code{measure.memory} to \code{TRUE} turns on memory measurement:
#' \code{\link[base]{gc}} is called  directly before
#' and after the job and the difference is stored in the internal database. Note that this is just a rough estimate and does
#' neither work reliably for external code like C/C++ nor in combination with threading.
#'
#' Furthermore, the package provides support for inner parallelization using threading, sockets or MPI via the
#' package \pkg{parallelMap}.
#' If you set the resource \dQuote{pm.backend} to \dQuote{multicore}, \dQuote{socket} or \dQuote{mpi},
#' \code{\link[parallelMap]{parallelStart}} is called on the slave before the first job in the chunk is started
#' and \code{\link[parallelMap]{parallelStop}} is called after the last job terminated.
#' This way, the used resources for inner parallelization are set in the same place as the resources for the outer parallelization and
#' get automatically stored in the \code{\link{Registry}}.
#' The user function just has to call \code{\link[parallelMap]{parallelMap}} to start parallelization to use the configured backend.
#'
#' You may set the resource \code{ncpus} to control the number of CPUs to use in \pkg{parallelMap}.
#' \code{ncpus} defaults to the number of available CPUs (as reported by (see \code{\link[parallel]{detectCores}}))
#' on the executing machine for multicore and socket mode and defaults to the return value of \code{\link[Rmpi]{mpi.universe.size}-1} for MPI.
#' You can pass further options like \code{level} to \code{\link[parallelMap]{parallelStart}} via the named list \dQuote{pm.opts}.
#'
#' Note that your template must be set up to handle the parallelization, e.g. start R with \code{mpirun} or request the correct number of CPUs.
#'
#' Also note that if you have thousands of jobs, disabling the progress bar (\code{options(batchtools.progress = FALSE)})
#' can significantly increase the performance.
#'
#' @templateVar ids.default findNotSubmitted
#' @template ids
#' @param resources [\code{named list}]\cr
#'   Computational  resources for the batch jobs. The elements of this list
#'   (e.g. something like \dQuote{walltime} or \dQuote{nodes}) depend on your template file.
#'   See notes for reserved special resource names.
#'   Defaults can be set in the \code{\link{Registry}} via the variable \code{default.resources} as a named list.
#'   The setting can be made permanent for all future registries by setting this variable in your configuration file.
#'   Individual settings set via \code{resources} \code{resources} overrule those in \code{default.resources}.
#' @template reg
#' @return [\code{\link{data.table}}]. Table with columns \dQuote{job.id} and \dQuote{chunk}.
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @export
#' @examples
#' ### Example 1: Using memory measurement
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # Toy function which creates a large matrix and returns the column sums
#' fun = function(n, p) colMeans(matrix(runif(n*p), n, p))
#'
#' # Arguments to fun:
#' args = expand.grid(n = c(1e4, 1e5), p = c(10, 50))
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
#' \dontrun{
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
submitJobs = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE, sync = TRUE)
  assertList(resources, names = "strict")

  ids = convertIds(reg, ids, default = .findNotSubmitted(reg = reg), keep.extra = c("job.id", "chunk"))
  if (nrow(ids) == 0L)
    return(noids())

  if (is.null(ids$chunk)) {
    chunks = ids$chunk = seq_row(ids)
  } else {
    assertInteger(ids$chunk, any.missing = FALSE)
    chunks = sort(unique(ids$chunk))
  }

  on.sys = .findOnSystem(reg = reg)
  max.concurrent.jobs = NA_integer_
  if (on.sys[ids, .N, nomatch = 0L] > 0L)
    stopf("Some jobs are already on the system, e.g. %s", stri_join(head(on.sys[ids, nomatch = 0L]$job.id, 1L), collapse = ", "))
  if (!is.null(reg$max.concurrent.jobs)) {
    if (nrow(on.sys) + length(chunks) > reg$max.concurrent.jobs)
      max.concurrent.jobs = reg$max.concurrent.jobs
  }

  resources = insert(reg$default.resources, resources)
  if (!is.null(resources$pm.backend))
    assertChoice(resources$pm.backend, c("local", "multicore", "socket", "mpi"))
  assertList(resources$pm.opts, names = "unique", null.ok = TRUE)
  assertCount(resources$ncpus, positive = TRUE, null.ok = TRUE)
  assertFlag(resources$measure.memory, null.ok = TRUE)
  assertFlag(resources$chunks.as.arrayjobs, null.ok = TRUE)

  res.hash = digest::digest(resources)
  resource.hash = NULL
  res.id = reg$resources[resource.hash == res.hash, "resource.id", with = FALSE]$resource.id
  if (length(res.id) == 0L) {
    res.id = auto_increment(reg$resources$resource.id)
    reg$resources = rbind(reg$resources, data.table(resource.id = res.id, resource.hash = res.hash, resources = list(resources)))
    setkeyv(reg$resources, "resource.id")
  }
  on.exit(saveRegistry(reg))

  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)
  update = data.table(submitted = NA_integer_, started = NA_integer_, done = NA_integer_, error = NA_character_,
    memory = NA_real_, resource.id = res.id, batch.id = NA_character_, job.hash = NA_character_)

  default.wait = 5
  chunk = NULL
  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta", tokens = list(status = "Submitting"))

  for (ch in chunks) {
    wait = default.wait
    ids.chunk = ids[chunk == ch, "job.id", with = FALSE]
    jc = makeJobCollection(ids.chunk, resources = resources, reg = reg)
    if (reg$cluster.functions$store.job)
      writeRDS(jc, file = jc$uri, wait = TRUE)

    if (!is.na(max.concurrent.jobs)) {
      # count chunks or job.id
      while (uniqueN(ids[.findOnSystem(reg = reg), on = "job.id", nomatch = 0L], by = "job.id") >= max.concurrent.jobs) {
        pb$tick(0, tokens = list(status = "Waiting   "))
        Sys.sleep(wait)
        wait = wait * 1.025
      }
    }

    repeat {
      runHook(reg, "pre.submit")
      now = ustamp()
      submit = reg$cluster.functions$submitJob(reg = reg, jc = jc)

      if (submit$status == 0L) {
        update[,  c("submitted", "batch.id", "job.hash") := list(now, submit$batch.id, jc$job.hash)]
        reg$status[ids.chunk, names(update) := update]
        runHook(reg, "post.submit")
        break
      } else if (submit$status > 0L && submit$status < 100L) {
        # temp error
        pb$tick(0L, tokens = list(status = submit$msg))
        Sys.sleep(wait)
        wait = wait * 1.025
      } else if (submit$status > 100L && submit$status <= 200L) {
        # fatal error
        stopf("Fatal error occurred: %i. %s", submit$status, submit$msg)
      }
    }
    pb$tick(tokens = list(status = "Submitting"))
  }

  # return ids, registry is saved via on.exit()
  return(invisible(ids))
}
