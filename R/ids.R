allIds = function(reg) {
  reg$status[, "job.id"]
}

noIds = function() {
  data.table(job.id = integer(0L), key = "job.id")
}

castIds = function(ids, setkey = TRUE) {
  if (is.data.table(ids)) {
    qassert(ids$job.id, "X", .var.name = "column 'job.id'")

    if (!is.integer(ids$job.id)) {
      "!DEBUG [castIds]: Casting ids in data.table to integer"
      ids = copy(ids)
      ids$job.id = as.integer(ids$job.id)
    }

    if (setkey && !identical(key(ids), "job.id")) {
      "!DEBUG [castIds]: Setting missing key for ids table"
      ids = copy(ids)
      setkeyv(ids, "job.id")
    }

    return(ids)
  }

  if (is.data.frame(ids)) {
    "!DEBUG [castIds]: Casting ids from data.frame to data.table"
    ids$job.id = asInteger(ids$job.id, .var.name = "column 'job.id'")
    ids = as.data.table(ids)
    if (setkey)
      setkeyv(ids, "job.id")
    return(ids)
  }

  if (qtest(ids, "X")) {
    "!DEBUG [castIds]: Casting ids from vector to data.table"
    return(data.table(job.id = as.integer(ids), key = if (setkey) "job.id" else NULL))
  }

  stop("Format of 'ids' not recognized. Must be a data.frame with column 'job.id' or an integerish vector")
}

convertIds = function(reg, ids, default = NULL, keep.extra = character(0L), keep.order = FALSE) {
  if (is.null(ids))
    return(default)

  ids = castIds(ids, setkey = !keep.order)
  if (anyDuplicated(ids, by = "job.id"))
    stop("Duplicated ids provided")

  invalid = ids[!reg$status, on = "job.id", which = TRUE]
  if (length(invalid) > 0L) {
    info("Ignoring %i invalid job id%s", length(invalid), if (length(ids) > 1L) "s" else "")
    ids = ids[-invalid]
  }

  cols = if (length(keep.extra)) union("job.id", chintersect(keep.extra, names(ids))) else "job.id"
  ids[, cols, with = FALSE]
}

convertId = function(reg, id) {
  id = convertIds(reg, id)
  if (nrow(id) != 1L)
    stopf("You must provide exactly one valid id (%i provided)", nrow(id))
  return(id)
}

filter = function(x, y, cols) {
  if (is.null(y))
    return(x[, cols, with = missing(cols)])
  return(x[y, cols, on = key(x), nomatch = 0L, with = missing(cols)])
}
