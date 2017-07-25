updateRegistry = function(reg = getDefaultRegistry()) { # nocov start
  "!DEBUG [updateRegistry]: Running updateRegistry"
  pv = packageVersion("batchtools")
  if (identical(pv, reg$version))
    return(TRUE)

  if (is.null(reg$version) || reg$version < "0.9.0")
    stop("Your registry is too old.")

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
    fns = list.files(fp(reg$file.dir, "exports"), pattern = "\\.rds$", all.files = TRUE, no.. = TRUE)
    if (length(fns)) {
      info("Renaming export files")
      file.rename(
        fp(reg$file.dir, fns),
        fp(reg$file.dir, mangle(stri_sub(fns, to = -5L)))
      )
    }
  }

  if (reg$version < "0.9.1-9002" && inherits(reg, "ExperimentRegistry")) {
    info("Renaming problems and algorithm files")
    for (prob in getProblemIds(reg))
      file.rename(fp(reg$file.dir, "problems", sprintf("%s.rds", digest(prob))), getProblemURI(reg, prob))
    for (algo in getAlgorithmIds(reg))
      file.rename(fp(reg$file.dir, "algorithms", sprintf("%s.rds", digest(algo))), getAlgorithmURI(reg, algo))
  }

  reg$version = pv
  saveRegistry(reg)
} # nocov end
