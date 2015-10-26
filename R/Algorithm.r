#' @rdname ProblemAlgorithm
#' @export
addAlgorithm = function(name, fun, reg = getDefaultRegistry())  {
  assertRegistry(reg, writeable = TRUE)
  assertString(name)
  if (!stri_detect_regex(name, "^[[:alnum:]_.-]+$"))
    stopf("Illegal characters in problem name: %s", name)
  if (!is.null(fun)) {
    assertFunction(fun)
    f = names(formals(fun))
    if (!("..." %in% f || all(c("data", "problem") %in% f)))
      stop("The algorithm function must have either '...' or both 'data' and 'problem' as formal arguments")
  }

  algo = setClasses(list(fun = fun, name = name), "Algorithm")
  write(algo, file = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name)))
  reg$algorithms = union(reg$algorithms, name)
  saveRegistry(reg)
  invisible(algo)
}

#' @export
#' @rdname ProblemAlgorithm
removeAlgorithm = function(name, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  assertString(name)
  assertSubset(name, reg$algorithms)
  pars = NULL

  fns = file.path(reg$file.dir, "algorithms", sprintf("%s.rds", name))
  def.ids = reg$defs[vcapply(pars, "[[", "algo.name") == name, "def.id", with = FALSE]
  job.ids = reg$status[def.ids, "job.id", on = "def.id", nomatch = 0L, with = FALSE]

  info("Removing Algorithm '%s' and %i corresponding jobs ...", name, nrow(job.ids))
  file.remove(fns)
  reg$algorithms = setdiff(reg$algorithms, name)
  reg$defs = reg$defs[!def.ids]
  reg$status = reg$status[!job.ids]
  saveRegistry(reg)
  invisible(TRUE)
}
