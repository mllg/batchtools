#' @title Export Objects to the Slaves
#'
#' @description
#' Objects are saved in subdirectory \dQuote{exports} of the
#' \dQuote{file.dir} of \code{reg}.
#' They are automatically loaded and placed in the global environment
#' each time the registry is loaded or a job collection is executed.
#'
#' @param export [\code{list}]\cr
#'  Named list of objects to export.
#' @param unexport [\code{character}]\cr
#'  Vector of object names to un-export.
#' @template reg
#' @return [\code{data.table}] with name and uri to the exported objects.
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#'
#' # list exports
#' exports = batchExport(reg = tmp)
#' print(exports)
#'
#' # add a job and required exports
#' batchMap(function(x) x^2 + y + z, x = 1:3, reg = tmp)
#' exports = batchExport(export = list(y = 99, z = 1), reg = tmp)
#' print(exports)
#'
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' stopifnot(loadResult(1, reg = tmp) == 101)
#'
#' # Un-export z
#' exports = batchExport(unexport = "z", reg = tmp)
#' print(exports)
batchExport = function(export = list(), unexport = character(0L), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  assertList(export, names = "strict")
  assertCharacter(unexport, any.missing = FALSE, min.chars = 1L)

  path = file.path(reg$file.dir, "exports")

  if (length(export) > 0L) {
    nn = names(export)
    fn = file.path(path, sprintf("%s.rds", nn))
    found = file.exists(fn)
    if (any(!found))
      info("Exporting new objects: '%s'", stri_join(nn[!found], collapse = "','"))
    if (any(found))
      info("Overwriting previously exported object: '%s'", stri_join(nn[found], collapse = "','"))
    Map(writeRDS, object = export, file = fn)
  }

  if (length(unexport) > 0L) {
    fn = file.path(path, sprintf("%s.rds", unexport))
    found = file.exists(fn)
    if (any(found))
      info("Un-exporting exported objects: '%s'", stri_join(unexport[found], collapse = "','"))
    unlink(fn[found])
  }

  fns = list.files(path, pattern = "\\.rds")
  invisible(data.table(name = stri_sub(fns, to = -5L), uri = file.path(path, fns)))
}
