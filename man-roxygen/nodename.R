#' @param nodename [\code{character(1)}]\cr
#'  Nodename of the master host. All commands are send via SSH to this host. Only works iff
#'  \enumerate{
#'    \item{Passwordless authentication (e.g., via SSH public key authentication) is set up.}
#'    \item{The file directory is shared across machines, e.g. mounted via SSHFS.}
#'    \item{Either the absolute path to the \code{file.dir} is identical on the machines, or paths are provided relative to the home directory. Symbolic links should work.}
#'  }
