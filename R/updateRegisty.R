# internal function for early adopters.
# if everything works as intended, this routine is not
# needed in the future.
updateRegistry = function(reg = getDefaultRegistry()) { # nocov start
  "!DEBUG Running updateRegistry"
  if (!is.data.table(reg$tags)) {
    info("Adding tags table")
    reg$tags = data.table( job.id = integer(0L), tag = character(0L), key = "job.id")
  }

  path = file.path(reg$file.dir, "exports")
  if (!dir.exists(path)) {
    info("Creating missing directory for exports")
    dir.create(path)
  }

  if (!inherits(reg, "ExperimentRegistry")) {
    reg$defs$pars.hash = NULL
  } else {
    probs = list.files(file.path(reg$file.dir, "problems"), pattern = "\\.rds")
    for (p in probs) {
      if (!stri_detect_regex("^[[:alnum:]_.-]+_[a-z0-9]{32}\\.rds", p)) {
        name = stri_replace_last_fixed(p, ".rds", "")
        info("Renaming file for problem '%s'", name)
        file.rename(file.path(reg$file.dir, "problems", p), getProblemURI(reg, name))
      }
    }

    algos = list.files(file.path(reg$file.dir, "algorithms"), pattern = "\\.rds")
    for (a in algos) {
      if (!stri_detect_regex("^[[:alnum:]_.-]+_[a-z0-9]{32}\\.rds", a)) {
        name = stri_replace_last_fixed(a, ".rds", "")
        info("Renaming file for algorithm '%s'", name)
        file.rename(file.path(reg$file.dir, "algorithms", a), getAlgorithmURI(reg, name))
      }
    }
  }

  saveRegistry(reg)
} # nocov end
