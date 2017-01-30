#' @title Run OS Commands on Local or Remote Machines
#'
#' @description
#' This is a helper function to run arbitrary OS commands on local or remote machines.
#' The interface is similar to \code{\link[base]{system2}}, but it always returns the exit status
#' \emph{and} the output.
#'
#' @param sys.cmd [\code{character(1)}]\cr
#'   Command to run.
#' @param sys.args [\code{character}]\cr
#'   Arguments for \code{sys.cmd}.
#' @param stdin [\code{character(1)}]\cr
#'   Argument passed to \code{\link[base]{system2}}.
#' @param nodename [\code{character(1)}]\cr
#'   Name of the SSH node to run the command on. If set to \dQuote{localhost} (default), the command
#'   is not piped through SSH.
#' @return [\code{named list}] with \dQuote{exit.code} (integer) and \dQuote{output} (character).
#' @export
#' @family ClusterFunctionsHelper
#' @examples
#' \dontrun{
#' runOSCommand("ls")
#' runOSCommand("ls", "-al")
#' runOSCommand("notfound")
#' }
runOSCommand = function(sys.cmd, sys.args = character(0L), stdin = "", nodename = "localhost") {
  assertCharacter(sys.cmd, any.missing = FALSE, len = 1L)
  assertCharacter(sys.args, any.missing = FALSE)
  assertString(nodename, min.chars = 1L)

  if (nodename != "localhost") {
    sys.args = c(nodename, shQuote(stri_flatten(c(sys.cmd, sys.args), " ")))
    sys.cmd = "ssh"
  } else if (length(sys.args) == 0L) {
      sys.args = ""
  }

  "!DEBUG [runOSCommand]: cmd: `sys.cmd` `stri_flatten(sys.args, ' ')`"

  if (nzchar(Sys.which(sys.cmd))) {
    res = suppressWarnings(system2(command = sys.cmd, args = sys.args, stdin = stdin, stdout = TRUE, stderr = TRUE, wait = TRUE))
    output = as.character(res)
    exit.code = attr(res, "status") %??% 0L
  } else {
    output = "command not found"
    exit.code = 127L
  }

  "!DEBUG [runOSCommand]: OS result (stdin '`stdin`', exit code `exit.code`):"
  "!DEBUG [runOSCommand]: `paste0(output, sep = '\n')`"

  return(list(exit.code = exit.code, output = output))
}
