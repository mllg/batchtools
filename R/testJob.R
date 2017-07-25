#' @title Run Jobs Interactively
#'
#' @description
#' Starts a single job on the local machine.
#'
#' @template id
#' @param external [\code{logical(1)}]\cr
#'  Run the job in an external R session? If \code{TRUE}, starts a fresh R
#'  session on the local machine to execute the with \code{\link{execJob}}.
#'  You will not be able to use debug tools like \code{\link[base]{traceback}}
#'  or \code{\link[base]{browser}}.
#'
#'  If \code{external} is set to \code{FALSE} (default) on the other hand,
#'  \code{testJob} will execute the job in the current R session and the usual
#'  debugging tools work. However, spotting missing variable declarations (as they
#'  are possibly resolved in the global environment) is impossible.
#'  Same holds for missing package dependency declarations.
#'
#' @template reg
#' @return Returns the result of the job if successful.
#' @export
#' @family debug
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' batchMap(function(x) if (x == 2) xxx else x, 1:2, reg = tmp)
#' testJob(1, reg = tmp)
#' \dontrun{
#' testJob(2, reg = tmp)
#' }
testJob = function(id, external = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(external)
  id = convertId(reg, id)

  if (external) {
    td      = normalizePath(tempdir(), winslash = "/")
    fn.r    = fp(td, "batchtools-testJob.R")
    fn.jc   = fp(td, "batchtools-testJob.jc")
    fn.res  = fp(td, "batchtools-testJob.rds")

    writeRDS(makeJobCollection(id, reg = reg), file = fn.jc)
    brew(file = system.file(fp("templates", "testJob.tmpl"), package = "batchtools", mustWork = TRUE),
      output = fn.r, envir = list2env(list(jc = fn.jc, result = fn.res)))

    res = runOSCommand(Rscript(), normalizePath(fn.r, winslash = "/"))

    writeLines(res$output)
    if (res$exit.code == 0L)
      return(readRDS(fn.res))
    stopf("testJob() failed for job with id=%i. To properly debug, re-run with external=FALSE", id$job.id)
  } else {
    with_dir(reg$work.dir, {
      loadRegistryDependencies(reg, must.work = TRUE)
      execJob(job = makeJob(id, reg = reg))
    })
  }
}
