allIds = function(reg) {
  reg$status[, "job.id", with = FALSE]
}

noIds = function() {
  data.table(job.id = integer(0L), key = "job.id")
}

castIds = function(ids, setkey = TRUE) {
  if (is.data.table(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")

    if (!is.integer(ids$job.id)) {
      "!DEBUG Casting ids in data.table to integer"
      ids = copy(ids)
      ids$job.id = as.integer(ids$job.id)
    }

    if (setkey && !identical(key(ids), "job.id")) {
      "!DEBUG Setting missing key for ids table"
      ids = copy(ids)
      setkeyv(ids, "job.id")
    }

    return(ids)
  }

  if (is.data.frame(ids)) {
    "!DEBUG Casting ids from data.frame to data.table"
    ids$job.id = asInteger(ids$job.id, .var.name = "column 'job.id'")
    return(as.data.table(ids, key = if (setkey) "job.id" else NULL))
  }

  if (qtest(ids, "X")) {
    "!DEBUG Casting ids from vector to data.table"
    return(data.table(job.id = as.integer(ids), key = if (setkey) "job.id" else NULL))
  }

  stop("Format of 'ids' not recognized. Must be a data frame with column 'job.id' or an integerish vector")
}

convertIds = function(reg, ids, default = NULL, keep.extra = character(0L), keep.order = FALSE) {
  if (is.null(ids))
    return(default)

  ids = castIds(ids, setkey = !keep.order)
  if (anyDuplicated(ids, by = "job.id"))
    stop("Duplicated ids provided")

  if (length(keep.extra) > 0L && ncol(ids) > 1L) {
    sort = !keep.order || identical(key(ids), "job.id")
    keep.extra = intersect(keep.extra, names(ids))
    return(merge(ids, reg$status, all = FALSE, sort = sort, by = "job.id")[, union("job.id", keep.extra), with = FALSE])
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
