#' @title Create a Linux-Worker
#' @docType class
#' @format An \code{\link{R6Class}} generator object
#'
#' @description
#' \code{\link[R6]{R6Class}} to create local and remote linux workers.
#'
#' @field nodename Host name. Set via constructor.
#' @field ncpus Number of CPUs. Set via constructor and defaults to a heuristic which tries to detect the number of CPUs of the machine.
#' @field max.load Maximum load average (of the last 5 min). Set via constructor and defaults to the number of CPUs of the machine.
#' @field status Status of the worker; one of \dQuote{unknown}, \dQuote{available}, \dQuote{max.cpus} and \dQuote{max.load}.
#' @section Methods:
#' \describe{
#'  \item{\code{new(nodename, ncpus, max.load)}}{Constructor.}
#'  \item{\code{update(reg)}}{Update the worker status.}
#'  \item{\code{list(reg)}}{List running jobs.}
#'  \item{\code{start(reg, fn, outfile)}}{Start job collection in file \dQuote{fn} and output to \dQuote{outfile}.}
#'  \item{\code{kill(reg, batch.id)}}{Kill job matching the \dQuote{batch.id}.}
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
  public = list(
    nodename = NULL,
    ncpus = NULL,
    max.load = NULL,
    script = NULL,
    status = "unknown",

    initialize = function(nodename, ncpus = NULL, max.load = NULL) {
      if (testOS("windows"))
        stop("Windows is not supported by the Worker Class")

      self$nodename = assertString(nodename)
      if (!is.null(ncpus))
        ncpus = asCount(ncpus)
      if (!is.null(max.load))
        assertNumber(max.load)

      if (nodename == "localhost") {
        self$script = system.file("bin", "linux-helper", package = "batchtools")
      } else {
        args = c("-e", shQuote("message(\"[bt] --BOF--\\n\", \"[bt] \", system.file(\"bin/linux-helper\", package = \"batchtools\"), \"\\n[bt] --EOF--\\n\")"))
        res = runOSCommand("Rscript", args, nodename = nodename)
        script = private$filter_output(res)$output
        if (!testString(script, min.chars = 1L)) {
          stopf("Unable to locate helper script on SSH node '%s'. Is batchtools installed on the node?", nodename)
        }
      }

      self$ncpus = ncpus %??% as.integer(private$run("number-of-cpus")$output)
      self$max.load = max.load %??% self$ncpus
    },

    list = function(reg) {
      stri_join(self$nodename, "#", stri_trim_both(private$run(c("list-jobs", reg$file.dir))$output))
    },

    start = function(reg, fn, outfile) {
      private$run(c("start-job", fn, outfile))
    },

    kill = function(reg, batch.id) {
      pid = stri_split_fixed(batch.id, "#", n = 2L)[[1L]][2L]
      cfKillJob(reg, self$script, c("kill-job", pid))
    },

    update = function(reg) {
      "!DEBUG [Worker]: Updating Worker '`self$nodename`'"
      res = private$run(c("status", reg$file.dir))
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
  ),

  private = list(
    filter_output = function(res) {
      output = stri_trim_both(res$output)
      marker = stri_detect_regex(output, "^\\[bt\\] --[BE]OF--$")
      if (sum(marker) != 2L) {
        stopf("runOSCommand failed: Expected BOF+EOF markers for '%s %s', but got:\n %s",
          res$sys.cmd, stri_flatten(res$sys.args, " "), stri_flatten(res$output, "\n") %??% "")
      }
      info = stri_startswith_fixed(output, "[bt]") & !marker
      res$output = stri_trim_left(stri_sub(output[info], 5L))
      res
    },

    run = function(args) {
      private$filter_output(runOSCommand(self$script, args, nodename = self$nodename))
    }
  )
)
