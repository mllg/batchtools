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
#' Setting \code{measure.memory} to \code{TRUE} turns on memory measurement: \code{\link[base]{gc}} is called  directly before
#' and after the job and the difference is stored in the internal database. Note that this is just a rough estimate and does
#' neither work reliably for external code like C/C++ nor in combination with inner parallelization and threading.
#'
#' Furthermore, the package provides support for inner parallelization using threads, sockets or MPI.
#' I.e., if a \code{\link{JobCollection}} starts on the slave, there are two ways for further parallelization:
#' Either execute multiple jobs in the chunk in parallel, or let each single job parallelize itself.
#'
#' For the first case, \pkg{batchtools} is responsible for the parallelization.
#' You can enable parallelization on the chunk level by setting the resource \code{inner.mode} to \dQuote{chunk}
#' and \code{inner.ncpus} to the desired number of CPUs to use. Furthermore, you can select a backend
#' by setting \code{inner.backend} to one of \dQuote{multicore}, \dQuote{socket} or \dQuote{mpi}.
#' The resource \code{inner.ncpus} defaults to the number of available CPUs (as reported by
#' (see \code{\link[parallel]{detectCores}}))
#' on the executing machine for multicore  and socket mode and defaults to the return value of
#' \code{\link[Rmpi]{mpi.universe.size}} for MPI.
#' The backend defaults to \code{socket} for Windows and to \dQuote{multicore} otherwise.
#'
#' In the second case, the provided user function must explicitly start parallelization.
#' However, \pkg{batchtools} provides built-in support for \pkg{parallelMap}.
#' If you set \code{inner.mode} to \dQuote{pm}, \code{\link[parallelMap]{parallelStart}} is called before the first job
#' is started and \code{\link[parallelMap]{parallelStop}} is called after the last job in the chunk is terminated.
#' This way, the used resources for inner parallelization are set like the resources for the outer parallelization and
#' get automatically stored in the \code{\link{Registry}}. Again, you may set \code{inner.ncpus} and \code{inner.backend}
#' (defaults are identical to the defaults of parallelization on chunk level).
#' The user provided function must only call \code{\link[parallelMap]{parallelMap}} for parallelization.
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
submitJobs = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  syncRegistry(reg)
  assertList(resources, names = "strict")
  ids = asJobTable(reg, ids, default = .findNotSubmitted(reg), keep.extra = TRUE)
  if (nrow(ids) == 0L)
    return(data.table(job.id = integer(0L), key = "job.id"))
  drop = setdiff(names(ids), c("job.id", "chunk"))
  if (length(drop) > 0L)
    ids[, drop := NULL, with = FALSE]

  on.sys = .findOnSystem(reg = reg)
  if (nrow(on.sys[ids, nomatch = 0L]) > 0L)
    stopf("Some jobs are already on the system, e.g. %s", stri_join(head(on.sys[ids, nomatch = 0L]$job.id, 1L), collapse = ", "))

  if (is.null(ids$chunk)) {
    chunks = ids$chunk = seq_row(ids)
  } else {
    assertInteger(ids$chunk, any.missing = FALSE)
    chunks = sort(unique(ids$chunk))
  }

  max.concurrent.jobs = NA_integer_
  if (!is.null(reg$max.concurrent.jobs)) {
    if (nrow(on.sys) + length(chunks) > reg$max.concurrent.jobs)
      max.concurrent.jobs = reg$max.concurrent.jobs
  }

  resources = insert(reg$default.resources, resources)
  if (!is.null(resources$inner.mode))
    assertChoice(resources$inner.mode, c("chunk", "pm"))
  if (!is.null(resources$inner.ncpus))
    assertCount(resources$inner.ncpus, positive = TRUE)
  if (!is.null(resources$inner.backend))
    assertChoice(resources$inner.backend, c("sequential", "multicore", "socket", "mpi"))
  if (!is.null(resources$measure.memory))
    assertFlag(resources$measure.memory)

  res.hash = digest::digest(resources)
  resource.hash = NULL
  res.id = reg$resources[resource.hash == res.hash, "resource.id", with = FALSE]$resource.id
  if (length(res.id) == 0L) {
    res.id = auto_increment(reg$resources$resource.id)
    reg$resources = rbind(reg$resources, data.table(resource.id = res.id, resource.hash = res.hash, resources = list(resources)))
    setkeyv(reg$resources, "resource.id")
  }
  on.exit(saveRegistry(reg))

  wait = 5
  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)
  update = data.table(submitted = NA_integer_, started = NA_integer_, done = NA_integer_, error = NA_character_,
    memory = NA_real_, resource.id = res.id, batch.id = NA_character_, job.hash = NA_character_)

  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta", tokens = list(status = "Submitting"))
  for (ch in chunks) {
    ids.chunk = ids[ids$chunk == ch, nomatch = 0L, "job.id", with = FALSE]
    jc = makeJobCollection(ids.chunk, resources = resources, reg = reg)
    if (reg$cluster.functions$store.job)
      writeRDS(jc, file = jc$uri, wait = TRUE)

    if (!is.na(max.concurrent.jobs)) {
      # count chunks or job.id (unique works on the key of ids)
      while (uniqueN(ids[.findOnSystem(reg = reg), on = "job.id", nomatch = 0L]) >= max.concurrent.jobs) {
        pb$tick(0, tokens = list(status = "Waiting   "))
        Sys.sleep(5)
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
      } else if (submit$status > 100L && submit$status <= 200L) {
        # fatal error
        stopf("Fatal error occurred: %i. %s", submit$status, submit$msg)
      }
    }
    pb$tick(tokens = list(status = "Submitting"))
  }

  ### return ids (on.exit handler kicks now in to submit the remaining messages)
  syncRegistry(reg = reg, save = FALSE)
  setkeyv(ids, "job.id")
  return(invisible(ids))
}
