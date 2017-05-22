#' @param template [\code{character(1)}]\cr
#'   Either a path to a brew template file (with extension \dQuote{tmpl}), or a short descriptive name enabling the following heuristic for the file lookup:
#'   \enumerate{
#'     \item \dQuote{batchtools.[template].tmpl} in the current working directory.
#'     \item \dQuote{[template].tmpl} in the user config directory (see \code{\link[rappdirs]{user_config_dir}}); on linux this is usually \dQuote{~/.config/batchtools/[template].tmpl}.
#'     \item \dQuote{.batchtools.[template].tmpl} in the home directory.
#'     \item \dQuote{[template].tmpl} in the package installation directory in the subfolder \dQuote{templates}.
#'   }
#'   Here, the default for \code{template} is \dQuote{<%= cf.name %>}.
#'   Alternatively, the template itself can be provided as a single string (including at least one newline \dQuote{\\n}).
