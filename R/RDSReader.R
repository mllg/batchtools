RDSReader = R6Class("RDSReader",
  cloneable = FALSE,
  public = list(
    cache = list(),
    use.cache = NA,

    initialize = function(use.cache = FALSE) {
      self$use.cache = use.cache
    },

    get = function(uri, slot = NA_character_) {
      read = function(uri) if (file.exists(uri)) readRDS(uri) else NULL

      # no cache used, read object from disk and return
      if (!self$use.cache)
        return(read(uri))

      # not slotted:
      # look up object in cache. If not found, add to cache. Return cached object
      if (is.na(slot)) {
        if (! uri %chin% names(self$cache))
          self$cache[[uri]] = read(uri)
        return(self$cache[[uri]])
      }

      # slotted:
      # object is stored in cache[[slot]] as list(obj = [cached obj], uri = uri)
      if (is.null(self$cache[[slot]]) || self$cache[[slot]]$uri != uri)
        self$cache[[slot]] = list(obj = read(uri), uri = uri)
      return(self$cache[[slot]]$obj)
    },

    clear = function() {
      self$cache = list()
    }
  )
)
