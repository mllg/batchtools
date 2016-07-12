#' @title Search for a Configuration File in Predefined Locations
#'
#' @description
#' Searches for a valid configuration file in the following locations and returns the first hit:
#' \enumerate{
#'   \item In the current working directory: \code{[getwd()]/[name]}
#'   \item In the home directory (prefixed with a dot): \code{~/.[name]}
#' }
#' @param name [\code{character(1)}]\cr
#'   File name of the config file.
#' @return [\code{character}]: location of first hit, empty character vector otherwise.
#' @export
#' @examples
#' # returns the batchtools configuration file
#' # (from current working dir or home dir)
#' findConfFile("batchtools.conf.R")
findConfFile = function(name) {
  uri = file.path(getwd(), name)
  if (file.exists(uri))
    return(uri)

  uri = npath("~", paste0(".", name))
  if (file.exists(uri))
    return(uri)

  return(character(0L))
}
