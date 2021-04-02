<!-- README.md is generated from README.Rmd. Please edit that file -->

# {lightning}

[![CRAN
status](https://www.r-pkg.org/badges/version/lightning)](https://cran.r-project.org/package=lightning)

## Overview

{lightning} allows the user to generate deviates from different
distributions.

{lightning} is a personal learning project. I wanted to learn more about
Scala invocations using {sparklyr} and chose generating random deviates
from distributions using the [RandomRDDs singleton
object](https://github.com/apache/spark/blob/v3.1.1/mllib/src/main/scala/org/apache/spark/mllib/random/RandomRDDs.scala).

## Installation

You can install:

-   the development version from
    [GitHub](https://github.com/nathaneastwood/lightning) with

``` r
# install.packages("remotes")
remotes::install_github("nathaneastwood/lightning")
```

## Usage

{lightning} provides two methods for generating random variates.
Firstly, we can generate N values from a `Distribution` class:

``` r
sc <- sparklyr::spark_connect(master = "local")

library(lightning)

norm <- Normal$new(sc = sc, size = 10L, num_partitions = 1L, seed = 1L)
norm$count()
# [1] 10
norm$collect()
#  [1] -0.7364418  1.1537268  0.4631666  1.7794325  0.3503825 -1.2078423
#  [7]  0.1825577 -0.2811541  0.1794811 -1.4066039
norm$first()
# [1] -0.7364418
norm$get_num_partitions()
# [1] 1
```

Secondly we can generate single values from a `Generator` class:

``` r
norm_gen <- NormalGenerator$new(sc = sc)
norm_gen$set_seed(1L)
norm_gen$next_value()
# [1] -1.032273
```

## TODO

-   Allow the ability to
    [`map()`](https://spark.apache.org/docs/2.4.3/api/java/org/apache/spark/rdd/RDD.html#map-scala.Function1-scala.reflect.ClassTag-)
    the distributions.
-   Convert the RandomRDDs to a Spark `DataFrame`.

## Acknowledgments

[jozefhajnala](https://github.com/jozefhajnala) and
[yitao-li](https://github.com/yitao-li) for their help on this topic.
