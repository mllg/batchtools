#' @rdname ProblemAlgorithm
#' @export
addAlgorithm = function(name, fun, reg = getDefaultRegistry())  {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, instance, ...) instance
  } else {
    assertFunction(fun, args = c("job", "data", "instance"))
  }

  algo = setClasses(list(fun = fun, name = name), "Algorithm")
  writeRDS(algo, file = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name)))
  levels(reg$defs$algorithm) = union(levels(reg$defs$algorithm), name)
  saveRegistry(reg)
  invisible(algo)
}

#' @export
#' @rdname ProblemAlgorithm
removeAlgorithm = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name)
  assertSubset(name, levels(reg$defs$algorithm))
  algorithm = NULL

  fns = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name))
  def.ids = reg$defs[algorithm == name, "def.id", with = FALSE]
  job.ids = reg$status[def.ids, "job.id", on = "def.id", nomatch = 0L, with = FALSE]

  info("Removing Algorithm '%s' and %i corresponding jobs ...", name, nrow(job.ids))
  file.remove(fns)
  reg$defs = reg$defs[!def.ids]
  reg$status = reg$status[!job.ids]
  reg$defs$algorithm = droplevel(reg$defs$algorithm, name)
  saveRegistry(reg)
  invisible(TRUE)
}
