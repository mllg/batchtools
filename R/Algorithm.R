#' @rdname ProblemAlgorithm
#' @export
addAlgorithm = function(name, fun = NULL, reg = getDefaultRegistry())  {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name, min.chars = 1L)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (is.null(fun)) {
    fun = function(job, data, instance, ...) instance
  } else {
    assert(checkFunction(fun, args = c("job", "data", "instance")), checkFunction(fun, args = "..."))
  }

  algo = setClasses(list(fun = fun, name = name), "Algorithm")
  writeRDS(algo, file = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name)))
  reg$defs$algorithm = addlevel(reg$defs$algorithm, name)
  saveRegistry(reg)
  invisible(algo)
}

#' @export
#' @rdname ProblemAlgorithm
removeAlgorithm = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE, running.ok = FALSE)
  assertString(name)
  assertSubset(name, levels(reg$defs$algorithm))

  fn = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name))
  algorithm = NULL
  def.ids = reg$defs[algorithm == name, "def.id", with = FALSE]
  job.ids = filter(def.ids, reg$status, "job.id")

  info("Removing Algorithm '%s' and %i corresponding jobs ...", name, nrow(job.ids))
  file.remove(fn)
  reg$defs = reg$defs[!def.ids]
  reg$status = reg$status[!job.ids]
  reg$defs$algorithm = droplevel(reg$defs$algorithm, name)
  sweepRegistry(reg)
  invisible(TRUE)
}

#' @export
#' @rdname ProblemAlgorithm
getAlgorithmIds = function(reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  levels(reg$defs$algorithm)
}
