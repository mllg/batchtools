#' @title Inspect Log Files
#'
#' @description
#' Opens a pager to the stored log file. For customization, see \code{\link[base]{file.show}}.
#' @template id
#' @template reg
#' @export
#' @family debug
#' @return Nothing.
showLog = function(id, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  id = asJobTable(reg, id)
  if (nrow(id) != 1L)
    stopf("You must provide exactly 1 id (%i provided)", nrow(id))
  lines = readLog(id, reg = reg)

  log.file = file.path(tempdir(), sprintf("%i.log", id$job.id))
  writeLines(text = lines, con = log.file)
  file.show(log.file, delete.file = TRUE)
}
