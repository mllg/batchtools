#' @title Run Jobs Interactively
#'
#' @description
#' Starts a single job in the current R session.
#' Debugging tools like \code{\link[base]{traceback}} should work.
#'
#' @template id
#' @param fresh.session [\code{logical(1)}]\cr
#'  Run the job in a fresh R session? If \code{TRUE}, starts a fresh R
#'  session on the local machine to execute the with \code{\link{execJob}}.
#'  You will not be able to use debug tools like \code{\link[base]{traceback}}
#'  or \code{\link[base]{browser}}.
#'  If \code{fresh.session} is set to \code{FALSE} (default), \code{testJob} will
#'  execute the job in the current R session and you are unable to spot
#'  missing variable declarations (possibly resolved in the global environment) as well
#'  as forgotten package dependencies.
#' @template reg
#' @return [ANY]. Returns the result of the job if successful.
#' @export
#' @family debug
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(function(x) if (x == 2) xxx else x, 1:2, reg = reg)
#' testJob(1, reg = reg)
#' \dontrun{
#'  testJob(2, reg = reg)
#' }
testJob = function(id, fresh.session = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(fresh.session)
  id = assertJobIds(asJobIds(reg, id), single.id = TRUE)
  job = makeJob(id, reg = reg)

  if (fresh.session) {
    fn = tempfile("testjob_")
    fn.r   = stri_join(fn, ".r")
    fn.job = stri_join(fn, ".job")
    fn.res = stri_join(fn, ".rds")
    fn.tmpl = system.file(file.path("templates", "testJob.tmpl"), package = "batchtools", mustWork = TRUE)

    writeRDS(job, file = fn.job)
    brew::brew(file = fn.tmpl, output = fn.r, envir = list2env(list(job = fn.job, result = fn.res)))
    res = runOSCommand("Rscript", fn.r, stop.on.exit.code = FALSE)

    if (res$exit.code == 0L) {
      writeLines(res$output)
      return(readRDS(fn.res))
    } else {
      stopf("testJob() failed for job with id=%i. To properly debug, re-run with fresh.session = FALSE", id$job.id)
    }
  } else {
    # FIXME: switch WD ... use withr?
    loadRegistryPackages(reg$packages, reg$namespaces)
    loadExtraFiles(reg$extra.files)
    execJob(job = job)
  }
}
