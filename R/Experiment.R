Experiment = R6Class("Experiment",
  public = list(
    initialize = function(cache, id, pars, repl, seed, resources, prob.name, algo.name) {
      self$cache = cache
      self$id = id
      self$pars = pars
      self$repl = repl
      self$seed = seed
      self$resources = resources
      self$prob.name = prob.name
      self$algo.name = algo.name
    },
    id = NULL,
    pars = NULL,
    repl = NULL,
    seed = NULL,
    resources = NULL,
    cache = NULL,
    prob.name = NULL,
    algo.name = NULL
  ),
  active = list(
    problem = function() self$cache$get(id = "..problem..", file.path("problems", self$prob.name)),
    algorithm = function() self$cache$get(file.path("algorithms", self$algo.name)),
    instance = function() {
      p = self$problem
      seed = if (is.null(p$seed)) self$seed else p$seed + self$repl - 1L
      wrapper = function(...) p$fun(job = self, data = p$data, ...)
      with_seed(seed, do.call(wrapper, self$pars$prob.pars))
    }
  ),
  cloneable = FALSE
)
