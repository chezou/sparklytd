#' Download td-spark jar
#'
#' Since file size of td-spark jar is larger than GitHub maximum size,
#' this command enables to download jar within sparklytd directory.
#'
#' @param dest_path The destination path where jar will be downloaded to.
#' @param force_download Flag for force download
#'
#' @examples
#' \dontrun{
#' download_jar()
#' }
#'
#' @importFrom utils download.file
#'
#' @export
download_jar <- function(dest_path = NULL, force_download = FALSE) {
  if (is.null(dest_path)) {
    dest_path <- file.path(system.file(package="sparklytd"), "java")
  }
  td_spark_version = "19.7.0"

  download_url <- sprintf("https://s3.amazonaws.com/td-spark/td-spark-assembly_2.11-%s.jar", td_spark_version)
  dest_file <- file.path(dest_path, basename(download_url))

  if (file.exists(dest_file) && !force_download) {
    stop("jar is already downloaded. Abort.")
  }

  if (!dir.exists(dirname(dest_file))) {
    dir.create(dirname(dest_file), recursive = TRUE)
  }

  download.file(download_url, destfile = dest_file)
}


#' Read a Treasure Data table into a Spark DataFrame
#'
#' @param sc A \code{spark_connection}.
#' @param name The name to assign to the newly generated table on Spark.
#' @param source Source name of the table on TD. Example: \samp{"sample_datasets.www_access"}
#' @param options A list of strings with additional options.
#' @param repartition The number of partitions used to distribute the
#'   generated table. Use 0 (the default) to avoid partitioning.
#' @param memory Boolean; should the data be loaded eagerly into memory? (That
#'   is, should the table be cached?)
#' @param overwrite Boolean; overwrite the table with the given name if it
#'   already exists?
#'
#' @details You can read TD table through td-spark. You have to set \code{spark.td.apikey},
#' \code{spark.serializer} appropreately.
#'
#' @family Spark serialization routines
#'
#' @examples
#' \dontrun{
#' config <- spark_config()
#'
#' config$spark.td.apikey <- Sys.getenv("TD_API_KEY")
#' config$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#' config$spark.sql.execution.arrow.enabled <- "true"
#'
#' sc <- spark_connect(master = "local", config = config)
#'
#' www_access <-
#'   spark_read_td(
#'   sc,
#'   name = "www_access",
#'   source = "sample_datasets.www_access")
#' }
#'
#' @export
spark_read_td <- function(sc,
                          name,
                          source,
                          options = list(),
                          repartition = 0,
                          memory = TRUE,
                          overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- spark_data_read_generic(sc, "com.treasuredata.spark", "format", options) %>%
    invoke("load", source)

  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Write a Spark DataFrame to Treasure Data
#'
#' @param x A Spark DataFrame or dplyr operation
#' @param name The name to write table.
#' @param options A list of strings with additional options.
#' @param mode A \code{character} element. Specifies the behavior when data or
#'   table already exists. Supported values include: 'error', 'append', 'overwrite' and
#'   'ignore'. Notice that 'overwrite' will also change the column structure.
#' @param partition_by A \code{character} vector. Partitions the output by the given columns on the file system.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark serialization routines
#'
#' @importFrom sparklyr spark_write_source
#' @examples
#' \dontrun{
#' config <- spark_config()
#'
#' config$spark.td.apikey <- Sys.getenv("TD_API_KEY")
#' config$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#' config$spark.sql.execution.arrow.enabled <- "true"
#'
#' sc <- spark_connect(master = "local", config = config)
#'
#' spark_mtcars <- dplyr::copy_to(sc, mtcars, "spark_mtcars", overwrite = TRUE)
#'
#' spark_write_td(
#'   spark_mtcars,
#'   name = "mydb.mtcars",
#'   mode = "overwrite"
#' )
#' }
#'
#' @export
spark_write_td <- function(x,
                           name,
                           mode = NULL,
                           options = list(),
                           partition_by = NULL,
                           ...) {
  UseMethod("spark_write_td")
}

#' @export
spark_write_td.tbl_spark <- function(x,
                                     name,
                                     mode = NULL,
                                     options = list(),
                                     partition_by = NULL,
                                     ...) {
  # td-spark API can't accept upper case column names
  x <- dplyr::rename_all(x, function(x){ tolower(x) })
  if (is.null(options[["table"]])) options[["table"]] <- name

  spark_write_source(
    x,
    "com.treasuredata.spark",
    mode = mode,
    options = options,
    partition_by = partition_by,
    ...
  )
}

#' @export
spark_write_td.spark_jobj <- function(x,
                                      name,
                                      mode = NULL,
                                      options = list(),
                                      partition_by = NULL,
                                      ...) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  # td-spark API can't accept upper case column names
  x <- invoke(x, "toDF", lapply(invoke(x, "columns"), function(x){tolower(x)}))

  if (is.null(options[["table"]])) options[["table"]] <- name

  spark_write_source(
    x,
    "com.treasuredata.spark",
    mode = mode,
    options = options,
    partition_by = partition_by,
    ...
  )
}
