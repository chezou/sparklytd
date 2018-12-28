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

# Read data on TD
df <- spark_read_td(sc, "www_access", "sample_datasets.www_access")
df %>% count()

# Copy R data.frame to TD
iris_tbl <- copy_to(sc, iris)
spark_write_td(iris_tbl, "aki.iris", mode="overwrite")
```

