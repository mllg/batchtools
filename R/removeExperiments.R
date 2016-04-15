removeExperiments = function(ids = integer(0L), reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg, writeable = TRUE)
  ids = asJobTable(reg, ids)

  info("Removing %i Experiments", nrow(ids))
  reg$status = reg$status[!ids]

  i = which(reg$defs$def.id %nin% reg$status$def.id)
  if (length(i) > 0L) {
    info("Cleaning up %i job definitions", length(i))
    reg$defs = reg$defs[-i]
  }

  sweepRegistry(reg)
  return(ids)
}
