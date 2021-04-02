# -- RandomRDDs ----------------------------------------------------------------

#' @noRd
RandomRDDs <- R6::R6Class(
  classname = "RandomRDDs",
  public = list(
    #'
    initialize = function(sc) {
      self$spark_connection <- sc
      self$spark_context <- sparklyr::spark_context(sc = sc)
      self$rdd <- sparklyr::invoke_static(
        sc = sc,
        class = "org.apache.spark.mllib.random.RandomRDDs$",
        method = "MODULE$"
      )
    },
    rdd = NULL,
    spark_connection = NULL,
    spark_context = NULL
  )
)

# -- Methods -------------------------------------------------------------------

#' Generate random deviates from an exponential distribution
#'
#' @export
Exponential <- R6::R6Class(
  classname = "Exponential",
    public = list(
    #' @description Create a new `Exponential` object.
    #' @param sc A `spark_connection`.
    #' @param mean `integer(1)`. The mean (`1 / lambda`) for the exponential
    #' distribution.
    #' @param size `integer(1)`. The number of deviates to generate.
    #' @param num_partitions `integer(1)`. Number of partitions in the RDD
    #' (default: `sc.defaultParallelism`).
    #' @param seed `integer(1)`. The seed to set.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   exp <- Exponential$new(
    #'     sc = sc,
    #'     mean = 2,
    #'     size = 10L,
    #'     num_partitions = 1L,
    #'     seed = 1L
    #'   )
    #' }
    #' @return A new `Exponential` object which is a `RDD[Double]` comprised of
    #' comprised of i.i.d. samples ~ `Exp(mean).`.
    initialize = function(sc, mean, size, num_partitions, seed) {
      stopifnot(sparklyr::connection_is_open(sc))
      stopifnot(is.numeric(mean), length(mean) == 1)
      stopifnot(is.integer(size), length(size) == 1)
      stopifnot(is.integer(num_partitions), length(num_partitions) == 1)
      stopifnot(is.integer(seed), length(seed) == 1)
      private$random_rdds <- RandomRDDs$new(sc = sc)
      private$values <- sparklyr::invoke(
        jobj = private$random_rdds$rdd,
        method = "exponentialRDD",
        private$random_rdds$spark_context,
        mean,
        size,
        num_partitions,
        seed
      )
    },
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   exp$collect()
    #' }
    collect = function() rdd_collect(values = private$values),
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   exp$collect()
    #' }
    count = function() rdd_count(values = private$values),
    #' @description Count the number of deviates.
    #' @examples
    #' \dontrun{
    #'   exp$count()
    #' }
    first = function() rdd_first(values = private$values),
    #' @description Get the number of partitions
    #' @examples
    #' \dontrun{
    #'   exp$get_num_partitions()
    #' }
    get_num_partitions = function() {
      rdd_get_num_partitions(values = private$values)
    }
  ),
  private = list(
    random_rdds = NULL,
    values = NULL
  )
)

#' Generate random deviates from a normal distribution
#'
#' @export
Normal <- R6::R6Class(
  classname = "Normal",
    public = list(
    #' @description Create a new `Normal` object.
    #' @param sc A `spark_connection`.
    #' @param size `integer(1)`. The number of deviates to generate.
    #' @param num_partitions `integer(1)`. Number of partitions in the RDD
    #' (default: `sc.defaultParallelism`).
    #' @param seed `integer(1)`. The seed to set.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   norm <- Normal$new(
    #'     sc = sc,
    #'     size = 10L,
    #'     num_partitions = 1L,
    #'     seed = 1L
    #'   )
    #' }
    #' @return A new `Normal` object which is a `RDD[Double]` comprised of
    #' comprised of i.i.d. samples ~ `N(0.0, 1.0).`.
    initialize = function(sc, size, num_partitions, seed) {
      stopifnot(sparklyr::connection_is_open(sc))
      stopifnot(is.integer(size), length(size) == 1)
      stopifnot(is.integer(num_partitions), length(num_partitions) == 1)
      stopifnot(is.integer(seed), length(seed) == 1)
      private$random_rdds <- RandomRDDs$new(sc = sc)
      private$values <- sparklyr::invoke(
        jobj = private$random_rdds$rdd,
        method = "normalRDD",
        private$random_rdds$spark_context,
        size,
        num_partitions,
        seed
      )
    },
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   norm$collect()
    #' }
    collect = function() rdd_collect(values = private$values),
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   norm$collect()
    #' }
    count = function() rdd_count(values = private$values),
    #' @description Count the number of deviates.
    #' @examples
    #' \dontrun{
    #'   norm$count()
    #' }
    first = function() rdd_first(values = private$values),
    #' @description Get the number of partitions
    #' @examples
    #' \dontrun{
    #'   norm$get_num_partitions()
    #' }
    get_num_partitions = function() {
      rdd_get_num_partitions(values = private$values)
    }
  ),
  private = list(
    random_rdds = NULL,
    values = NULL
  )
)

#' Generate random deviates from a uniform distribution
#'
#' @export
Uniform <- R6::R6Class(
  classname = "Uniform",
  public = list(
    #' @description Create a new `Uniform` object.
    #' @param sc A `spark_connection`.
    #' @param size `integer(1)`. The number of deviates to generate.
    #' @param num_partitions `integer(1)`. Number of partitions in the RDD
    #' (default: `sc.defaultParallelism`).
    #' @param seed `integer(1)`. The seed to set.
    #' @examples
    #' \dontrun{
    #'   sc <- sparklyr::spark_connect(master = "local")
    #'   unif <- Uniform$new(
    #'     sc = sc,
    #'     size = 10L,
    #'     num_partitions = 1L,
    #'     seed = 1L
    #'   )
    #' }
    #' @return A new `Uniform` object which is a `RDD[Double]` comprised of
    #' i.i.d. samples ~ `U(0.0, 1.0)`.
    initialize = function(sc, size, num_partitions, seed) {
      stopifnot(sparklyr::connection_is_open(sc))
      stopifnot(is.integer(size), length(size) == 1)
      stopifnot(is.integer(num_partitions), length(num_partitions) == 1)
      stopifnot(is.integer(seed), length(seed) == 1)
      private$random_rdds <- RandomRDDs$new(sc = sc)
      private$values <- sparklyr::invoke(
        jobj = private$random_rdds$rdd,
        method = "uniformRDD",
        private$random_rdds$spark_context,
        size,
        num_partitions,
        seed
      )
    },
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   unif$collect()
    #' }
    collect = function() rdd_collect(values = private$values),
    #' @description Collect the random deviates into R.
    #' @examples
    #' \dontrun{
    #'   unif$collect()
    #' }
    count = function() rdd_count(values = private$values),
    #' @description Count the number of deviates.
    #' @examples
    #' \dontrun{
    #'   unif$count()
    #' }
    first = function() rdd_first(values = private$values),
    #' @description Get the number of partitions
    #' @examples
    #' \dontrun{
    #'   unif$get_num_partitions()
    #' }
    get_num_partitions = function() {
      rdd_get_num_partitions(values = private$values)
    }
  ),
  private = list(
    random_rdds = NULL,
    values = NULL
  )
)

# -- Distribution Methods ------------------------------------------------------

#' @details
#' More information and methods can be found at
#' \url{https://spark.apache.org/docs/2.4.3/api/java/org/apache/spark/rdd/RDD.html#map-scala.Function1-scala.reflect.ClassTag-}
#' @param values An R6 `Distribution` class such as `Normal` or `Uniform`.
#' @name rdd_methods

#' @inheritParams rdd_methods
#' @noRd
rdd_collect <- function(values) {
  sparklyr::invoke(jobj = values, method = "collect")
}

#' @inheritParams rdd_methods
#' @noRd
rdd_count <- function(values) {
  sparklyr::invoke(jobj = values, method = "count")
}

#' @inheritParams rdd_methods
#' @noRd
rdd_first <- function(values) {
  sparklyr::invoke(jobj = values, method = "first")
}

#' @inheritParams rdd_methods
#' @noRd
rdd_get_num_partitions <- function(values) {
  sparklyr::invoke(jobj = values, method = "getNumPartitions")
}
