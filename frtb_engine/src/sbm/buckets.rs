use polars::prelude::*;

pub fn sbm_buckets () -> Expr {
    when(col("RiskClass").eq(lit("FX")))
    .then( col("Bucket").fill_null(col("RiskFactor")))
    .otherwise(col("Bucket"))
}