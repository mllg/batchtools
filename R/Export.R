#' @title Export Objects to the Slaves
#'
#' @description
#' Objects are saved in subdirectory \dQuote{exports} of the
#' \dQuote{file.dir} of \code{reg}.
#' They are automatically loaded and put into the global environment
#' each time the registry is loaded on the master as well as on the slaves.
#'
#' @param x [\code{list}]\cr
#'  Named list of objects to export.
#'  Set to \code{NULL} to unexport the object.
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
#' # add job depending on exports
#' batchMap(function(x) x^2 + y + z, x = 1:3, reg = tmp)
#' exports = batchExport(list(y = 99, z = 1), reg = tmp)
#' print(exports)
#'
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#' loadResult(1, reg = tmp)
#'
#' # Un-export z
#' exports = batchExport(list(z = NULL), reg = tmp)
#' print(exports)
batchExport = function(x = list(), reg = getDefaultRegistry()) {
  assertRegistry(reg, writeable = TRUE)
  assertList(x, names = "strict")

  path = file.path(reg$file.dir, "exports")
  if (!dir.exists(path))
    dir.create(path)

  if (length(x) > 0L) {
    nn = names(x)
    fn = file.path(path, sprintf("%s.rds", nn))
    found = file.exists(fn)
    null = vlapply(x, is.null)

    i = found & !null
    if (any(i))
      info("Overwriting previously exported object '%s'", stri_join(nn[i], collapse = "','"))
    Map(writeRDS, object = x[!null], file = fn[!null])
    unlink(fn[found & null])
  }

  fns = list.files(path, pattern = "\\.rds")
  invisible(data.table(name = stri_sub(fns, to = -5L), uri = file.path(path, fns)))
}
