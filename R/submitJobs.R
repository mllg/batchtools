#' @title Submit jobs to the Batch Systems
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
#' @templateVar ids.default findNotSubmitted
#' @template ids
#' @param resources [\code{named list}]\cr
#'   Computational  resources for the batch jobs. The elements of this list
#'   (e.g. something like \dQuote{walltime} or \dQuote{nodes}) depend on your template file.
#'   The resources \code{chunk.ncpus} and \code{measure.memory} are reserved for internal functionality:
#'   The setting \code{chunk.ncpus} is used to determine the number of CPUs to execute jobs in a
#'   chunk in parallel via \code{\link[parallel]{mcparallel}} (or \code{\link[snow]{sendCall}} on Windows).
#'   If not set, \code{chunk.ncpus} defaults to 1 (sequential execution).
#'   The second reserved resource, \code{measure.memory}, can be set to \code{TRUE} to enable the measure of
#'   memory requirements using \code{\link[base]{gc}}. But note that the reported values are quite
#'   heuristic and may not reflect the real memory requirements. Furthermore, measuring memory with
#'   \code{\link[base]{gc}} in parallel is impossible, thus this feature is disabled if \code{chunk.ncpus}
#'   is greater than one.
#'
#'   Defaults for all resources, the reserved as well as the those depending on your template, can be stored in the \code{\link{Registry}} in
#'   \code{default.resources} (e.g., using a configuration file).
#'   Individual settings set via \code{resources} \code{resources} overrule those in \code{default.resources}.
#' @template reg
#' @return [\code{\link{data.table}}]. Table with columns \dQuote{job.id} and \dQuote{chunk}.
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

  if (!is.null(resources$chunk.ncpus))
    assertCount(resources$chunk.ncpus, positive = TRUE)
  if (!is.null(resources$measure.memory))
    assertFlag(resources$measure.memory)

  res.hash = digest::digest(resources)
  if (nrow(reg$resources[res.hash, nomatch = 0L]) == 0L) {
    reg$resources = rbind(reg$resources, data.table(resource.id = res.hash, resources = list(resources)))
    setkeyv(reg$resources, "resource.id")
  }
  on.exit(saveRegistry(reg))

  wait = 5
  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)
  update = data.table(submitted = NA_integer_, started = NA_integer_, done = NA_integer_, error = NA_character_,
    memory = NA_real_, resource.id = res.hash, batch.id = NA_character_, job.hash = NA_character_)

  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta", tokens = list(status = "Submitting"))
  for (ch in chunks) {
    ids.chunk = ids[ids$chunk == ch, nomatch = 0L, "job.id", with = FALSE]
    jc = makeJobCollection(ids.chunk, resources = resources, reg = reg)
    if (reg$cluster.functions$store.job)
      writeRDS(jc, file = jc$uri, wait = TRUE)

    if (!is.na(max.concurrent.jobs)) {
      # count chunks or job.id (unique works on the key of ids)
      while (uniqueN(ids[.findOnSystem(reg = reg), on = "job.id", nomatch = 0L]) >= max.concurrent.jobs) {
        pb$tick(0, tokens = list(status = "Waiting "))
        Sys.sleep(5)
      }
    }

    repeat {
      runHook(reg, "pre.submit")
      submit = reg$cluster.functions$submitJob(reg = reg, jc = jc)

      if (submit$status == 0L) {
        update[,  c("submitted", "batch.id", "job.hash") := list(now(), submit$batch.id, jc$job.hash)]
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
    pb$tick(tokens = list(status = "Submitting "))
  }

  ### return ids (on.exit handler kicks now in to submit the remaining messages)
  syncRegistry(reg = reg, save = FALSE)
  setkeyv(ids, "job.id")
  return(invisible(ids))
}
