#' Read TD table
#'
#' @param sc An active \code{spark_connection}.

spark_read_td <- function(sc,
                          name,
                          source,
                          readOptions = list(),
                          repartition = 0,
                          memory = TRUE,
                          overwrite = TRUE,
                          ...){
  if (overwrite && name %in% DBI::dbListTables(sc)) {
    DBI::dbRemoveTable(sc, name)
  }

  df <- sparklyr:::spark_data_read_generic(sc, "com.treasuredata.spark", "format") %>%
    invoke("load", source)

  sparklyr::invoke(df, "registerTempTable", name)

  dplyr::tbl(sc, name)
}
