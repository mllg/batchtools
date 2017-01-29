updateRegistry = function(reg = getDefaultRegistry()) { # nocov start
  "!DEBUG Running updateRegistry"
  pv = packageVersion("batchtools")
  update = FALSE

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
    update = TRUE
  }

  if (reg$version <= "0.9.2") {
    ### hotfix for timestamps
    if (is.integer(reg$status$submitted)) {
      info("Converting timestamps to numeric")
      for (x in c("submitted", "started", "done"))
        reg$status[[x]] = as.numeric(reg$status[[x]])
      update = TRUE
    }

    ### hotfix for log.file column
    if ("log.file" %nin% names(reg$status)) {
      reg$status$log.file = NA_character_
      update = TRUE
    }
  }

  if (update) {
    reg$version = pv
    saveRegistry(reg)
  }
} # nocov end
