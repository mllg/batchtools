# returns TRUE if the state possibly changed
updateRegistry = function(reg = getDefaultRegistry()) { # nocov start
  "!DEBUG [updateRegistry]: Running updateRegistry"
  pv = packageVersion("batchtools")
  if (identical(pv, reg$version))
    return(FALSE)

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
    if ("log.file" %chnin% names(reg$status)) {
      info("Adding column 'log.file'")
      reg$status[, ("log.file") := rep(NA_character_, .N)]
    }
  }

  if (reg$version < "0.9.1-9001") {
    ### hotfix for base32 encoding of exports
    fns = list.files(fs::path(reg$file.dir, "exports"), pattern = "\\.rds$", all.files = TRUE, no.. = TRUE)
    if (length(fns)) {
      info("Renaming export files")
      fs::file_move(
        fs::path(reg$file.dir, fns),
        fs::path(reg$file.dir, mangle(stri_sub(fns, to = -5L)))
      )
    }
  }

  if (reg$version < "0.9.1-9002" && inherits(reg, "ExperimentRegistry")) {
    info("Renaming problems and algorithm files")
    getProblemIds = function(reg) levels(reg$defs$problem)
    getAlgorithmIds = function(reg) levels(reg$defs$algorithm)

    for (prob in getProblemIds(reg))
      fs::file_move(fs::path(reg$file.dir, "problems", sprintf("%s.rds", digest(prob))), getProblemURI(reg, prob))
    for (algo in getAlgorithmIds(reg))
      fs::file_move(fs::path(reg$file.dir, "algorithms", sprintf("%s.rds", digest(algo))), getAlgorithmURI(reg, algo))
  }

  if (reg$version < "0.9.4-9001") {
    if ("job.name" %chnin% names(reg$status)) {
      info("Adding column 'job.name'")
      reg$status[, ("job.name") := rep(NA_character_, .N)]
    }
  }

  if (reg$version < "0.9.6-9001") {
    info("Updating registry internals")
    if (!inherits(reg, "ExperimentRegistry")) {
      setnames(reg$defs, "pars", "job.pars")
    } else {
      alloc.col(reg$defs, ncol(reg$defs) + 1L)
      reg$problems = levels(reg$defs$problem)
      reg$algorithms = levels(reg$defs$algorithm)
      reg$defs$problem = as.character(reg$defs$problem)
      reg$defs$algorithm = as.character(reg$defs$algorithm)
      reg$defs$prob.pars = lapply(reg$defs$pars, `[[`, "prob.pars")
      reg$defs$algo.pars = lapply(reg$defs$pars, `[[`, "algo.pars")
      reg$defs$pars = NULL
      info("Recalculating job hashes")
      reg$defs$pars.hash = calculateHash(reg$defs)
    }
  }

  if (reg$version < "0.9.7-9001") {
    if (inherits(reg, "ExperimentRegistry")) {
      info("Updating problems")
      for (id in reg$problems) {
        uri = getProblemURI(reg, id)
        p = readRDS(uri)
        p$cache = FALSE
        saveRDS(p, file = uri, version = 2L)
      }
    }
  }

  if (reg$version < "0.9.7-9002") {
    if (hasName(reg$status, "memory")) {
      info("Renaming memory column in data base")
      setnames(reg$status, "memory", "mem.used")
    }

    fns = list.files(dir(reg, "updates"), full.names = TRUE)
    if (length(fns) > 0L) {
      info("Renaming memory column in update files")
      updates = lapply(fns, function(fn) {
        x = try(readRDS(fn), silent = TRUE)
        if (is.error(x)) {
          fs::file_delete(x)
        } else {
          if (hasName(x, "memory")) {
            setnames(x, "memory", "mem.used")
            saveRDS(x, file = fn, version = 2L)
          }
        }
      })
    }
  }

  if (is.null(reg$compress)) {
    reg$compress = "gzip"
  }

  reg$version = pv
  return(TRUE)
} # nocov end
