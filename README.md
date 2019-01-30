# sparklytd

sparklytd is an extension to read and write TD data from/to R.

## Installation

You can install this package via devtools with:

``` r
install.packages("devtools")
devtools::install_github("chezou/sparklytd")
```

## Example

This is a basic example to handle TD data:

``` r
# Assuming TD API key is set in environment varraible "TD_API_KEY". 
library(sparklyr)
library(sparklytd)

# Before first connection, you need to download td-spark jar.
download_jar()

# Connect td-spark API
conf <- spark_config()
conf$spark.td.apikey=Sys.getenv("TD_API_KEY")
conf$spark.serializer="org.apache.spark.serializer.KryoSerializer"
conf$spark.sql.execution.arrow.enabled="true"

# If you want to use Java9, set this option
options(sparklyr.java9 = TRUE)

sc <- spark_connect(master = "local", config = conf)

library(dplyr)
# Read data on TD
df <- spark_read_td(sc, "www_access", "sample_datasets.www_access")
df %>% count()

# Copy R data.frame to TD
iris_tbl <- copy_to(sc, iris)
spark_write_td(iris_tbl, "aki.iris", mode="overwrite")

# Execute Presto SQL on TD
spark_read_td_presto(sc,
 "sample",
 "sample_datasets",
 "select count(1) from www_access") %>% collect()

# Run non-query Presto statements, e.g., CREATE TABLE, DROP TABLE, etc.
spark_execute_td_presto(sc,
 "sample_datasets",
 "create table if not exists orders (key bigint, status varchar, price double)")

# Execute Presto or Hive SQL as a regular TD job
spark_read_td_query(sc,
 "sample",
 "sample_datasets.www_access",
 "select count(1) from sample_datasets.www_access", engine = "presto") %>% collect()
```

