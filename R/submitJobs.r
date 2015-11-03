#' @title Submit jobs or chunks of jobs to batch system via cluster function.
#'
#' @description
#' Submits all jobs to the batch system using the resources provided via
#' \code{resources}.
#'
#' If an additional column \dQuote{chunk} is present in the table \code{ids},
#' the jobs will be grouped accordingly. See \code{\link{chunkIds}} for more
#' information.
#'
#' @templateVar ids.default findNotSubmitted
#' @template ids
#' @param resources [\code{named list}]\cr
#'   Computational  resources for the batch jobs. The elements of this list
#'   (e.g. something like \dQuote{walltime} or \dQuote{nodes}) depend on by
#'   your template file, with the exception of \code{ncpus}: This setting is
#'   used to determine the number of CPUs to use on a node to execute jobs in a
#'   chunk in parallel using \code{\link[parallel]{mclapply}}. If not set,
#'   \code{ncpus} defaults to 1 (sequential execution).
#'
#'   Defaults can be set in the \code{\link{Registry}} via
#'   \code{default.resources}. Settings in \code{resources} overrule those in
#'   \code{default.resources}.
#' @template reg
#' @return [\code{\link{data.table}}]. Table with columns \dQuote{job.id} and \dQuote{chunk}.
#' @export
submitJobs = function(ids = NULL, resources = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  syncRegistry(reg)
  assertList(resources, names = "strict")
  ids = asIds(reg, ids, default = .findNotSubmitted(reg), extra.cols = TRUE)
  drop = setdiff(names(ids), c("job.id", "chunk", "group"))
  if (length(drop) > 0L)
    ids[, drop := NULL, with = FALSE]

  on.sys = .findOnSystem(reg = reg)
  if (nrow(on.sys[ids, nomatch = 0L]) > 0L)
    stopf("Some jobs are already on the system, e.g. %s", paste0(head(on.sys[ids, nomatch = 0L]$job.id, 1L), collapse = ", "))

  if (is.null(ids$chunk)) {
    ids$chunk = seq_row(ids)
    chunks = seq_row(ids)
  } else {
    assertInteger(ids$chunk, any.missing = FALSE)
    chunks = sort(unique(ids$chunk))
  }
  chunk = NULL
  setkeyv(ids, "chunk")

  max.concurrent.jobs = NA_integer_
  if (!is.null(reg$max.concurrent.jobs)) {
    if (nrow(on.sys) + length(chunks) > reg$max.concurrent.jobs)
      max.concurrent.jobs = reg$max.concurrent.jobs
  }

  resources = insert(reg$default.resources, resources)
  resources = resources[order(names2(resources))]
  res.hash = digest::digest(resources)
  resources.hash = NULL
  res.id = head(reg$resources[resources.hash == res.hash, "resource.id", with = FALSE]$resource.id, 1L)
  if (length(res.id) == 0L) {
    res.id = if (nrow(reg$resources) > 0L) max(reg$resources$resource.id) + 1L else 1L
    reg$resources = rbind(reg$resources, data.table(resource.id = res.id, resources.hash = res.hash, resources = list(resources)))
    setkeyv(reg$resources, "resource.id")
  }

  on.exit(saveRegistry(reg))

  wait = 2
  info("Submitting %i jobs in %i chunks using cluster functions '%s' ...", nrow(ids), length(chunks), reg$cluster.functions$name)
  update = data.table(submitted = NA_integer_, started = NA_integer_, done = NA_integer_, error = NA_character_,
    memory = NA_real_, resource.id = res.id, batch.id = NA_character_, job.hash = NA_character_)

  pb = makeProgressBar(total = length(chunks), format = ":status [:bar] :percent eta: :eta", tokens = list(status = "Submitting"))
  for (ch in chunks) {
    ids.chunk = ids[chunk == ch, nomatch = 0L]
    jc = makeJobCollection(ids.chunk, resources = resources, reg = reg)
    if (reg$cluster.functions$store.job)
      write(jc, file = jc$uri, wait = TRUE)

    if (!is.na(max.concurrent.jobs)) {
      # count chunks or job.id (unique works on the key of ids)
      while (uniqueN(ids[.findOnSystem(reg = reg), on = "job.id", nomatch = 0L]) >= max.concurrent.jobs) {
        pb$tick(0, tokens = list(status = "Waiting "))
        Sys.sleep(5)
      }
    }

    repeat {
      submit = reg$cluster.functions$submitJob(reg = reg, jc = jc)

      if (submit$status == 0L) {
        update[,  c("submitted", "batch.id", "job.hash") := list(now(), submit$batch.id, jc$job.hash)]
        reg$status[ids.chunk, names(update) := update]
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
