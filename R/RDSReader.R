RDSReader = R6Class("RDSReader",
  cloneable = FALSE,
  public = list(
    cache = list(),
    use.cache = NA,

    initialize = function(use.cache = FALSE) {
      self$use.cache = use.cache
    },

    get = function(uri, slot = NULL) {
      read = function(uri) if (file.exists(uri)) readRDS(uri) else NULL
      if (self$use.cache) {
        nn = slot %??% uri
        if (! nn %chin% names(self$cache))
          self$cache[[nn]] = read(uri)
        self$cache[[nn]]
      } else {
        read(uri)
      }
    },

    clear = function() {
      self$cache = list()
    }
  )
)
