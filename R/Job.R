Job = R6Class("Job",
  cloneable = FALSE,
  public = list(
    initialize = function(cache, id, pars, seed, resources) {
      self$cache = cache
      self$id = id
      self$job.pars = pars
      self$seed = seed
      self$resources = resources
    },
    id = NULL,
    job.pars = NULL,
    seed = NULL,
    resources = NULL,
    cache = NULL
  ),
  active = list(
    pars = function() c(self$job.pars, self$cache$get("more.args")),
    fun = function() self$cache$get("user.function")
  )
)
