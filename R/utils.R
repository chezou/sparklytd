spark_data_read_generic <- get("spark_data_read_generic",
                               envir = asNamespace("sparklyr"))

spark_data_write_generic <- get("spark_data_write_generic",
                               envir = asNamespace("sparklyr"))

spark_sqlresult_from_dplyr <- get("spark_sqlresult_from_dplyr",
                                  envir = asNamespace("sparklyr"))

spark_expect_jobj_class <- get("spark_expect_jobj_class",
                              envir = asNamespace("sparklyr"))

spark_partition_register_df <- get("spark_partition_register_df",
                                   envir = asNamespace("sparklyr"))

spark_remove_table_if_exists <- get("spark_remove_table_if_exists",
                                    envir = asNamespace("sparklyr"))