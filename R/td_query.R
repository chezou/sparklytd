#' Read Treasure Data data from a query
#'
#' @inheritParams spark_read_td
#' @param source Data base name of the table on TD. Example: \samp{"sample_datasets"}
#' @param query A SQL to execute
#' @param engine An engine name. "presto", "hive", "pig" can be acceptable.
#'
#' @details You can execute queries to TD through td-spark. You have to set \code{spark.td.apikey},
#' \code{spark.serializer} appropreately.
#'
#' @family Spark serialization routines
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#' config <- spark_config()
#'
#' config$spark.td.apikey <- Sys.getenv("TD_API_KEY")
#' config$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#' config$spark.sql.execution.arrow.enabled <- "true"
#'
#' sc <- spark_connect(master = "local", config = config)
#'
#' df <- spark_read_td_query(sc,
#'   "sample",
#'   "sample_datasets",
#'   "select count(1) from www_access",
#'   engine = "presto") %>% collect()
#' }
#'
#' @export
spark_read_td_query <- function(sc,
                                name,
                                source,
                                query,
                                engine = "presto",
                                options = list(),
                                repartition = 0,
                                memory = TRUE,
                                overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(sc, name)

  method <- paste0(engine, "Job")
  df <- invoke_td(sc) %>% invoke(method, query, source) %>% invoke("df")

  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Read Treasure Data data from Presto via api-presto gateway
#'
#' @inheritParams spark_read_td
#' @param source Data base name of the table on TD. Example: \samp{"sample_datasets"}
#' @param query A SQL to execute
#'
#' @details You can execute queries to TD through td-spark. You have to set \code{spark.td.apikey},
#' \code{spark.serializer} appropreately.
#'
#' @family Spark serialization routines
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#' config <- spark_config()
#'
#' config$spark.td.apikey <- Sys.getenv("TD_API_KEY")
#' config$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#' config$spark.sql.execution.arrow.enabled <- "true"
#'
#' sc <- spark_connect(master = "local", config = config)
#'
#' df <- spark_read_td_presto(sc,
#'   "sample",
#'   "sample_datasets",
#'   "select count(1) from www_access") %>% collect()
#' }
#'
#' @export
spark_read_td_presto <- function(sc,
                                 name,
                                 source,
                                 query,
                                 options = list(),
                                 repartition = 0,
                                 memory = TRUE,
                                 overwrite = TRUE) {
  if (overwrite) spark_remove_table_if_exists(sc, name)

  df <- invoke_td(sc) %>% invoke("presto", query, source)

  spark_partition_register_df(sc, df, name, repartition, memory)
}

#' Read Treasure Data data from Presto via api-presto gateway
#'
#' @param sc A \code{spark_connection}.
#' @param source Data base name of the table on TD. Example: \samp{"sample_datasets"}
#' @param query A SQL to execute
#' @param options A list of strings with additional options.
#'
#' @details You can execute queries to TD through td-spark. You have to set \code{spark.td.apikey},
#' \code{spark.serializer} appropreately.
#'
#' @family Spark serialization routines
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#' config <- spark_config()
#'
#' config$spark.td.apikey <- Sys.getenv("TD_API_KEY")
#' config$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#' config$spark.sql.execution.arrow.enabled <- "true"
#'
#' sc <- spark_connect(master = "local", config = config)
#'
#' spark_execute_td_presto(sc,
#'   "sample_datasets",
#'   "create table if not exists orders (key bigint, status varchar, price double)")
#' }
#'
#' @export
spark_execute_td_presto <- function(sc,
                                    source,
                                    query,
                                    options = list()) {
  invoke_td(sc) %>% invoke("executePresto", query, source)
}
