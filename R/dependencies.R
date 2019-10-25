spark_dependencies <- function(spark_version, scala_version, ...) {
  td_spark_version = "19.7.0"

  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/td-spark-assembly_%s-%s.jar", scala_version, td_spark_version),
        package = "sparklytd"
      )
    ),
    packages = c(
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
