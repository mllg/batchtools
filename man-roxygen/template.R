#' @param template [\code{character(1)}]\cr
#'   Path to a brew template file that can be used for the job description.
#'   Alternatively, the template as a single string (including at least one newline \dQuote{\\n}).
#'   Defaults to a heuristic which looks for a template file in the following locations and picks
#'   the first one found:
#'   \enumerate{
#'     \item \dQuote{batchtools.<%= cf.name %>.tmpl} in the current working directory.
#'     \item \dQuote{<%= cf.name %>.tmpl} in the user config directory (see \code{\link[rappdirs]{user_config_dir}}); on linux is is usually \dQuote{~/.config/batchtools/<%= cf.name %>.tmpl}.
#'     \item \dQuote{.batchtools.<%= cf.name %>.tmpl} in the home directory.
#'     \item \dQuote{<%= cf.name %>.default.tmpl} in the package installation directory in the subfolder \dQuote{templates} (this probably does not exist for your cluster system or needs adaptation).
#'   }
