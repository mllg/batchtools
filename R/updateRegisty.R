updateRegistry = function(reg = getDefaultRegistry()) { # nocov start
  "!DEBUG [updateRegistry]: Running updateRegistry"
  pv = packageVersion("batchtools")
  if (identical(pv, reg$version))
    return(TRUE)

  ### hotfix for versions before first cran release.
  ### this will be removed in the future
  if (reg$version < "0.9.0") {
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
  }

  if (reg$version < "0.9.1-9000") {
    ### hotfix for timestamps
    if (is.integer(reg$status$submitted)) {
      info("Converting timestamps to numeric")
      for (x in c("submitted", "started", "done"))
        reg$status[[x]] = as.numeric(reg$status[[x]])
    }

    ### hotfix for log.file column
    if ("log.file" %nin% names(reg$status)) {
      reg$status$log.file = NA_character_
    }
  }

  if (reg$version < "0.9.1-9001") {
    ### hotfix for base32 encoding of exports
    fns = list.files(file.path(reg$file.dir, "exports"), pattern = "\\.rds$", all.files = TRUE, no.. = TRUE)
    if (length(fns)) {
      info("Renaming export files")
      file.rename(
        file.path(reg$file.dir, fns),
        file.path(reg$file.dir, mangle(stri_sub(fns, to = -5L)))
      )
    }
  }

  if (reg$version < "0.9.1-9002" && inherits(reg, "ExperimentRegistry")) {
    info("Renaming problems and algorithm files")
    for (prob in getProblemIds(reg))
      file.rename(file.path(reg$file.dir, "problems", sprintf("%s.rds", digest(prob))), getProblemURI(reg, prob))
    for (algo in getAlgorithmIds(reg))
      file.rename(file.path(reg$file.dir, "algorithms", sprintf("%s.rds", digest(algo))), getAlgorithmURI(reg, algo))
  }

  reg$version = pv
  saveRegistry(reg)
} # nocov end
