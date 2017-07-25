findConfFile = function() {
  x = "batchtools.conf.R"
  if (file.exists(x))
    return(normalizePath(x, winslash = "/"))

  x = fp(user_config_dir("batchtools", expand = FALSE), "config.R")
  if (file.exists(x))
    return(x)

  x = normalizePath(fp("~", ".batchtools.conf.R"), winslash = "/", mustWork = FALSE)
  if (file.exists(x))
    return(x)

  return(character(0L))
}

setSystemConf = function(reg, conf.file) {
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$default.resources = list()
  reg$temp.dir = tempdir()

  if (length(conf.file) > 0L) {
    assertString(conf.file)
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)

    assertClass(reg$cluster.functions, "ClusterFunctions")
    assertList(reg$default.resources, names = "unique")
    if (!dir.exists(reg$temp.dir))
      dir.create(reg$temp.dir, recursive = TRUE)
  } else {
    info("No configuration file found")
  }
}
