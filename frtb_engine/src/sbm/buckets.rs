//! TODO for CSR if Bucket is empy - fill null based on CSR Sector and Credit Quality
//! 
use polars::prelude::*;

pub fn sbm_buckets () -> Expr {
    when(col("RiskClass").eq(lit("FX")))
    //Note Offshore: if RF is THOUSD, bucket should be THBUSD
    .then( col("BucketBCBS").fill_null(col("RiskFactor")))
    .otherwise(col("BucketBCBS"))
}

#[cfg(feature = "CRR2")]
pub fn sbm_buckets_crr2 () -> Expr {
    col("BucketCRR2").fill_null(col("BucketBCBS"))
}