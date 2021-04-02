# -- Generators ----------------------------------------------------------------

#' Distribution Generators
#'
#' Generator values from a distribution.
#'
#' @details
#' See \url{http://spark.apache.org/docs/latest/api/scala/org/apache/spark/mllib/random/index.html}
#' for more information.
#'
#' @seealso
#' [ExponentialGenerator()], [StandardNormalGenerator()], [UniformGenerator()]
#'
#' @importFrom R6 R6Class
#' @name Generators
NULL

#' Generate values from a standard normal distribution.
#'
#' @seealso [Generators]
#' @export
ExponentialGenerator <- R6::R6Class(
  classname = "ExponentialGenerator",
  public = list(
    #' @description Create a new `ExponentialGenerator` object.
    #' @param sc A `spark_connection`.
    #' @param mean A `numeric(1)`.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   exp_gen <- ExponentialGenerator$new(sc = sc, mean = 2)
    #' }
    #' @return A new `ExponentialGenerator` object.
    initialize = function(sc, mean) {
      stopifnot(is.numeric(mean), length(mean) == 1L)
      private$gen <- invoke_gen(sc = sc, gen = "ExponentialGenerator", mean)
    },
    #' @description Get the next value
    #' @examples
    #' \dontrun{
    #'   exp_gen$next_value()
    #' }
    #' @return A `numeric(1)` i.i.d.
    next_value = function() gen_next_value(private$gen),
    #' @description Set random seed
    #' @param seed `integer(1)`. The unit.
    #' @examples
    #' \dontrun{
    #'   exp_gen$set_seed(1L)
    #' }
    #' @return `NULL`, invisibly.
    set_seed = function(seed) gen_set_seed(gen = private$gen, seed = seed)
  ),
  private = list(
    gen = NULL
  )
)

#' Generate values from a standard normal distribution.
#'
#' @seealso [Generators]
#' @export
NormalGenerator <- R6::R6Class(
  classname = "NormalGenerator",
  public = list(
    #' @description Create a new `NormalGenerator` object.
    #' @param sc A `spark_connection`.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   norm_gen <- NormalGenerator$new(sc = sc)
    #' }
    #' @return A new `NormalGenerator` object.
    initialize = function(sc) {
      private$gen <- invoke_gen(sc = sc, gen = "StandardNormalGenerator")
    },
    #' @description Get the next value
    #' @examples
    #' \dontrun{
    #'   norm_gen$next_value()
    #' }
    #' @return A `numeric(1)` i.i.d.
    next_value = function() gen_next_value(private$gen),
    #' @description Set random seed
    #' @param seed `integer(1)`. The unit.
    #' @examples
    #' \dontrun{
    #'   norm_gen$set_seed(1L)
    #' }
    #' @return `NULL`, invisibly.
    set_seed = function(seed) gen_set_seed(gen = private$gen, seed = seed)
  ),
  private = list(
    gen = NULL
  )
)

#' Generate values from a uniform distribution.
#'
#' @seealso [Generators]
#' @export
UniformGenerator <- R6::R6Class(
  classname = "UniformGenerator",
  public = list(
    #' @description Create a new `UniformGenerator` object.
    #' @param sc A `spark_connection`.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   unif_gen <- UniformGenerator$new(sc = sc)
    #' }
    #' @return A new `UniformGenerator` object.
    initialize = function(sc) {
      private$gen <- invoke_gen(sc = sc, gen = "UniformGenerator")
    },
    #' @description Get the next value
    #' @examples
    #' \dontrun{
    #'   unif_gen$next_value()
    #' }
    #' @return A `numeric(1)` i.i.d.
    next_value = function() gen_next_value(private$gen),
    #' @description Set random seed
    #' @param seed `integer(1)`. The unit.
    #' @examples
    #' \dontrun{
    #'   unif_gen$set_seed(1L)
    #' }
    #' @return `NULL`, invisibly.
    set_seed = function(seed) gen_set_seed(gen = private$gen, seed = seed)
  ),
  private = list(
    gen = NULL
  )
)

# -- Invocation ----------------------------------------------------------------

#' Invoke A Generator
#'
#' @param sc A `spark_connection`
#' @param gen `character(1)`. The name of the generator.
#' @param ... Additional parameters to be passed to the generator class.
#'
#' @return An object of class `c("spark_jobj", "shell_jobj")`
#'
#' @importFrom sparklyr invoke_new
#' @noRd
invoke_gen <- function(sc, gen, ...) {
  sparklyr::invoke_new(
    sc = sc,
    class = paste0("org.apache.spark.mllib.random.", gen),
    ...
  )
}

# -- Methods -------------------------------------------------------------------

#' Get The Next Value
#' @param gen An R6 Generator class.
#' @return A `numeric(1)` i.i.d.
#' @noRd
gen_next_value <- function(gen) {
  sparklyr::invoke(jobj = gen, method = "nextValue")
}

#' Set Random Seed
#' @param gen An R6 Generator class.
#' @param seed `integer(1)`. The unit.
#' @return `NULL`, invisibly.
#' @noRd
gen_set_seed <- function(gen, seed) {
  stopifnot(is.integer(seed), length(seed) == 1L)
  invisible(sparklyr::invoke(jobj = gen, method = "setSeed", seed))
}
