#' @title Create a Linux-Worker
#' @docType class
#' @format An \code{\link{R6Class}} generator object
#'
#' @description
#' \code{\link[R6]{R6Class}} to create local and remote linux workers.
#'
#' @field nodename Host name. Set via constructor.
#' @field ncpus Number of VPUs of worker. Set via constructor.
#' @field max.load Maximum load average (of the last 5 min). Set via constructor.
#' @field debug Verbose output. Set via constructor.
#' @field status Status of the worker; one of \dQuote{unknown}, \dQuote{available}, \dQuote{max.cpus} and \dQuote{max.load}.
#' @section Methods:
#' \describe{
#'  \item{\code{new(nodename, ncpus, max.load, debug)}}{Constructor.}
#'  \item{\code{update(reg)}}{Update the worker status.}
#'  \item{\code{list(reg)}}{List running jobs.}
#'  \item{\code{start(reg, fn, outfile)}}{Start job collection in file \dQuote{fn} and output to \dQuote{outfile}.}
#'  \item{\code{kill(reg, pid)}}{Kill job matching the process id.}
#' }
#' @return [\code{\link{Worker}}].
#' @export
#' @examples
#' \dontrun{
#' # create a worker for the local machine and use 4 CPUs.
#' Worker$new("localhost", ncpus = 4)
#' }
Worker = R6Class("Worker",
  cloneable = FALSE,
  private = list(
    script = NULL,
    debug = NULL
  ),

  public = list(
    nodename = NULL,
    ncpus = NULL,
    max.load = NULL,
    status = "unknown",

    initialize = function(nodename, ncpus = 0L, max.load = Inf, debug = FALSE) {
      if (testOS("windows"))
        stop("Windows is not supported by the Worker Class")
      self$nodename = assertString(nodename)
      self$ncpus = as.integer(assertInt(ncpus))
      self$max.load = assertNumber(max.load, lower = 0)
      private$debug = assertFlag(debug)


      if (nodename == "localhost") {
        private$script = system.file("bin", "linux-helper", package = "batchtools")
      } else {
        args = c("-e", shQuote("message(system.file('bin/linux-helper', package = 'batchtools'))"))
        private$script = tail(runOSCommand("Rscript", args, nodename = nodename, debug = private$debug)$output, 1L)
      }

      if (self$ncpus == 0L)
        self$ncpus = as.integer(runOSCommand(private$script, "number-of-cpus", debug = private$debug)$output)
    },

    list = function(reg) {
      stri_trim_both(runOSCommand(private$script, c("list-jobs", reg$file.dir), debug = private$debug)$output)
    },

    start = function(reg, fn, outfile) {
      runOSCommand(private$script, c("start-job", fn, outfile), debug = private$debug)$output
    },

    kill = function(reg, pid) {
      runOSCommand(private$script, c("kill-job", pid), debug = private$debug)$exit.code == 0L
    },

    update = function(reg) {
      res = runOSCommand(private$script, "status", nodename = self$nodename, debug = private$debug)
      res = as.numeric(stri_split_regex(res$output, "\\s+")[[1L]])
      names(res) = c("load", "n.rprocs", "n.rprocs.50", "n.jobs")
      self$status = if (res["load"] > self$max.load) {
        "max.load"
      } else if (res["n.jobs"] >= self$ncpus) {
        "max.cpus"
      } else {
        "available"
      }
      return(res)
    }
  )
)
