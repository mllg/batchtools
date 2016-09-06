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
#'  If \code{external} is set to \code{FALSE} (default), \code{testJob} will
#'  execute the job in the current R session, and you are unable to spot
#'  missing variable declarations (possibly resolved in the global environment) as well
#'  as forgotten package dependencies.
#' @template reg
#' @return [ANY]. Returns the result of the job if successful.
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
    td      = npath(tempdir())
    fn.r    = file.path(td, sprintf("testjob_%i.R", id$job.id))
    fn.jc   = file.path(td, sprintf("testjob_%i.jc", id$job.id))
    fn.res  = file.path(td, sprintf("testjob_%i.rds", id$job.id))
    fn.tmpl = system.file(file.path("templates", "testJob.tmpl"), package = "batchtools", mustWork = TRUE)

    writeRDS(makeJobCollection(id, reg = reg), file = fn.jc, wait = TRUE)
    brew::brew(file = fn.tmpl, output = fn.r, envir = list2env(list(jc = fn.jc, result = fn.res)))
    res = runOSCommand(Rscript(), fn.r)

    writeLines(res$output)
    if (res$exit.code == 0L)
      return(readRDS(fn.res))
    stopf("testJob() failed for job with id=%i. To properly debug, re-run with external=FALSE", id$job.id)
  } else {
    loadRegistryDependencies(reg, switch.wd = TRUE)
    execJob(job = makeJob(id, reg = reg))
  }
}
