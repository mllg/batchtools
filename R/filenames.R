npath = function(path, must.work = TRUE) {
  if (stri_startswith_fixed(path, "~")) {
    # do not call normalizePath, we do not want to expand this paths relative to home
    if (must.work && !file.exists(path))
      stopf("Path '%s' not found", path)
    if (testOS("windows"))
      path = stri_replace_all_fixed(path, "\\", "/")
    return(path)
  }
  normalizePath(path, winslash = "/", mustWork = must.work)
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}
