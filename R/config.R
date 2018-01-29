findConfFile = function() {
  x = Sys.getenv("R_BATCHTOOLS_SEARCH_PATH")
  if (nzchar(x)) {
    x = fs::path(x, "batchtools.conf.R")
    if (fs::file_exists(x))
      return(fs::path_real(x))
  }

  x = "batchtools.conf.R"
  if (fs::file_exists(x))
    return(fs::path_real(x))

  x = fs::path(user_config_dir("batchtools", expand = FALSE), "config.R")
  if (fs::file_exists(x))
    return(x)

  x = fs::path("~", ".batchtools.conf.R")
  if (fs::file_exists(x))
    return(fs::path_real(x))

  return(character(0L))
}

setSystemConf = function(reg, conf.file) {
  reg$cluster.functions = makeClusterFunctionsInteractive()
  reg$default.resources = list()
  reg$temp.dir = fs::path_temp()

  if (length(conf.file) > 0L) {
    assertString(conf.file)
    info("Sourcing configuration file '%s' ...", conf.file)
    sys.source(conf.file, envir = reg, keep.source = FALSE)

    assertClass(reg$cluster.functions, "ClusterFunctions")
    assertList(reg$default.resources, names = "unique")
    fs::dir_create(reg$temp.dir)
  } else {
    info("No configuration file found")
  }
}
