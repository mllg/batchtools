#' @title Find a batchtools Configuration File
#'
#' @description
#' This functions returns the path to the first configuration file found in the following locations:
#'   \enumerate{
#'    \item{File \dQuote{batchtools.conf.R} in the path specified by the environment variable \dQuote{R_BATCHTOOLS_SEARCH_PATH}.}
#'    \item{File \dQuote{batchtools.conf.R} in the current working directory.}
#'    \item{File \dQuote{config.R} in the user configuration directory as reported by \code{rappdirs::user_config_dir("batchtools", expand = FALSE)} (depending on OS, e.g., on linux this usually resolves to \dQuote{~/.config/batchtools/config.R}).}
#'    \item{\dQuote{.batchtools.conf.R} in the home directory (\dQuote{~}).}
#'    \item{\dQuote{config.R} in the site config directory as reported by \code{rappdirs::site_config_dir("batchtools")} (depending on OS). This file can be used for admins to set sane defaults for a computation site.}
#'   }
#' @return [\code{character(1)}] Path to the configuration file or \code{NA} if no configuration file was found.
#' @keywords internal
#' @export
findConfFile = function() {
  x = Sys.getenv("R_BATCHTOOLS_SEARCH_PATH")
  if (nzchar(x)) {
    x = fs::path(x, "batchtools.conf.R")
    if (fs::file_access(x, "read"))
      return(fs::path_abs(x))
  }

  x = "batchtools.conf.R"
  if (fs::file_access(x, "read"))
    return(fs::path_abs(x))

  x = fs::path(user_config_dir("batchtools", expand = FALSE), "config.R")
  if (fs::file_access(x, "read"))
    return(x)

  x = fs::path("~", ".batchtools.conf.R")
  if (fs::file_access(x, "read"))
    return(fs::path_abs(x))

  x = fs::path(site_config_dir("batchtools"), "config.R")
  if (fs::file_access(x, "read"))
    return(x)

  return(NA_character_)
}

setSystemConf = function(reg, conf.file) {
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$default.resources = list()
  reg$temp.dir = fs::path_temp()

  if (!is.na(conf.file)) {
    assertString(conf.file)
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)

    assertClass(reg$cluster.functions, "ClusterFunctions")
    assertList(reg$default.resources, names = "unique")
    fs::dir_create(reg$temp.dir)
  } else {
    info("No readable configuration file found")
  }
}
