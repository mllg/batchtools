allIds = function(reg) {
  reg$status[, "job.id", with = FALSE]
}

noIds = function() {
  data.table(job.id = integer(0L), key = "job.id")
}

castIds = function(ids, setkey = TRUE, ensure.copy = FALSE) {
  if (is.data.table(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")
    if (setkey && !identical(key(ids), "job.id")) {
      ids = copy(ids)
      setkeyv(ids, "job.id")
    } else if (ensure.copy) {
      ids = copy(ids)
    }
  } else if (is.data.frame(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")
    ids = as.data.table(ids)
    if (setkey)
      setkeyv(ids, "job.id")
  } else if (qtest(ids, "X")) {
    ids = if (setkey) data.table(job.id = as.integer(ids), key = "job.id") else data.table(job.id = as.integer(ids))
  } else {
    stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
  }

  return(ids)
}

convertIds = function(reg, ids, default = NULL, keep.extra = FALSE, keep.order = FALSE) {
  if (is.null(ids))
    return(default)

  ids = castIds(ids, setkey = !keep.order)
  if (anyDuplicated(ids, by = "job.id"))
    stop("Duplicated ids provided")

  if (!identical(keep.extra, FALSE) && ncol(ids) > 1L) {
    i = ids[reg$status, on = "job.id", nomatch = 0L, which = TRUE]
    if (isTRUE(keep.extra))
      return(ids[i])
    keep.extra = intersect(keep.extra, names(ids))
    return(ids[i, keep.extra, with = FALSE])
  }
  return(reg$status[ids, "job.id", on = "job.id", nomatch = 0L, with = FALSE])
}

convertId = function(reg, id) {
  id = convertIds(reg, id)
  if (nrow(id) != 1L)
    stopf("You must provide exactly one id (%i provided)", nrow(id))
  return(id)
}

filter = function(x, y, cols) {
  if (is.null(y))
    return(x[, cols, with = missing(cols)])
  return(x[y, cols, on = key(x), nomatch = 0L, with = missing(cols)])
}
