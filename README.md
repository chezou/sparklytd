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

# Connect td-spark API
default_conf <- spark_config()
default_conf$spark.td.apikey=Sys.getenv("TD_API_KEY")
default_conf$spark.serializer="org.apache.spark.serializer.KryoSerializer"
default_conf$psark.sql.execution.arrow.enabled="true"

sc <- spark_connect(master = "local", config = default_conf)

# Read data on TD
df <- spark_read_td(sc, "www_access", "sample_datasets.www_access")
df %>% count()

# Copy R data.frame to TD
iris_tbl <- copy_to(sc, iris)
spark_write_td(iris_tbl, "aki.iris", mode="overwrite")
```

