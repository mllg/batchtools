#' @export
#' @keywords internal
updateRegistry = function(reg = getDefaultRegistry()) {
  if (!is.data.table(reg$tags))
    reg$tags = data.table( job.id = integer(0L), tag = character(0L), key = "job.id")

  probs = list.files(file.path(reg$file.dir, "problems"), pattern = "\\.rds")
  for (p in probs) {
    if (!stri_detect_regex("^[[:alnum:]_.-]+_[a-z0-9]{32}\\.rds", p)) {
      name = stri_replace_last_fixed(p, ".rds", "")
      file.rename(file.path(reg$file.dir, "problems", p), getProblemURI(reg, name))
    }
  }

  algos = list.files(file.path(reg$file.dir, "algorithms"), pattern = "\\.rds")
  for (a in algos) {
    if (!stri_detect_regex("^[[:alnum:]_.-]+_[a-z0-9]{32}\\.rds", a)) {
      name = stri_replace_last_fixed(a, ".rds", "")
      file.rename(file.path(reg$file.dir, "algorithms", a), getAlgorithmURI(reg, name))
    }
  }

  saveRegistry(reg)
}
