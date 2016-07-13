#' @param template [\code{character(1)}]\cr
#'   Path to a brew template file that can be used for the job description.
#'   Alternatively, the template as a single string (including at least one newline \dQuote{\\n}).
#'   Defaults to a heuristic which looks for a template file in the following locations and picks
#'   the first one found:
#'   (1) \dQuote{batchtools.<%= cf.name %>.tmpl} in the current working directory,
#'   (2) \dQuote{.batchtools.<%= cf.name %>.tmpl} in the home directory and finally
#'   (3) \dQuote{<%= cf.name %>.default.tmpl} in the package installation directory
#'   in the subfolder \dQuote{templates}.
