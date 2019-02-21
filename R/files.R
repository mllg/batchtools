dir = function(reg, what) {
  fs::path(fs::path_expand(reg$file.dir), what)
}

getResultFiles = function(x, ids) {
  UseMethod("getResultFiles")
}

getResultFiles.Registry = function(x, ids) {
  ids = if (is.atomic(ids)) ids else ids$job.id
  hash = x$status[list(ids), "job.hash", with = FALSE][[1L]]
  shard = if(isTRUE(x$sharding)) stri_sub(hash, 4L, 5L) else ""
  fs::path(dir(x, "results"), shard, sprintf("%i.rds", ids))
}

getResultFiles.JobCollection = function(x, ids) {
  ids = if (is.atomic(ids)) ids else ids$job.id
  shard = if(isTRUE(x$sharding)) stri_sub(x$job.hash, 4L, 5L) else ""
  fs::path(dir(x, "results"), shard, sprintf("%i.rds", ids))
}

getLogFiles = function(reg, ids) {
  job.hash = log.file = NULL
  ids = if (is.atomic(ids)) ids else ids$job.id
  tab = reg$status[list(ids), c("job.id", "job.hash", "log.file"), on = "job.id"]
  tab[is.na(log.file) & !is.na(job.hash), log.file := sprintf("%s.log", job.hash)]

  tab[!is.na(log.file),
    log.file := fs::path(dir(reg, "logs"), if(isTRUE(reg$sharding)) stri_sub(job.hash, 4L, 5L) else "", log.file)
  ]
  tab$log.file
}

getJobFiles = function(reg, hash) {
  shard = if(isTRUE(reg$sharding)) stri_sub(hash, 4L, 5L) else ""
  fs::path(reg$file.dir, "jobs", shard, sprintf("%s.rds", hash))
}

getExternalDirs = function(reg, ids) {
  fs::path(dir(reg, "external"), if (is.atomic(ids)) ids else ids$job.id)
}

mangle = function(x) {
  sprintf("%s.rds", base32_encode(x, use.padding = FALSE))
}

unmangle = function(x) {
  base32_decode(stri_sub(x, to = -5L), use.padding = FALSE)
}

file_remove = function(x) {
  fs::file_delete(x[fs::file_exists(x)])

  while(any(i <- fs::file_exists(x))) {
    Sys.sleep(0.5)
    fs::file_delete(x[i])
  }
}

file_mtime = function(x) {
  fs::file_info(x)$modification_time
}

writeRDS = function(object, file) {
  file_remove(file)
  dir = fs::path_dir(file)
  if (!fs::dir_exists(dir))
    fs::dir_create(dir)
  saveRDS(object, file = file, version = 2L)
  waitForFile(file, 300)
  invisible(TRUE)
}
