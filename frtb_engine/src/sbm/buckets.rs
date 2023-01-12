//! TODO for CSR if Bucket is empy - fill null based on CSR Sector and Credit Quality, if these are provided
use std::collections::BTreeMap;

use polars::prelude::*;

pub fn sbm_buckets(_: &BTreeMap<String, String>) -> Expr {
    //let offshore_onshore = conf
    //    .get("offshore_onshore_fx")
    //    .and_then(|x| serde_json::from_str::<HashMap<String, String>>(x).ok())
    //    .unwrap_or_default();

    when(
        col("RiskClass")
            .eq(lit("FX"))
            .or(col("RiskClass").eq(lit("GIRR"))),
    )
    .then(col("BucketBCBS").fill_null(
        col("RiskFactor"), // Code will panic on a long onshore-offshore when().then()
                           // Hence, such mapping has to be done separately
                           /*.map(
                           move |srs| {
                               let mut res = srs.utf8()?.to_owned();
                               for (k, v) in &offshore_onshore {
                                   res = res.replace(k, v)?;
                               }
                               Ok(res.into_series())
                           },
                           GetOutput::from_type(DataType::Utf8),
                           ) */
    ))
    .otherwise(col("BucketBCBS"))
}

#[cfg(feature = "CRR2")]
pub fn sbm_buckets_crr2() -> Expr {
    col("BucketCRR2").fill_null(col("BucketBCBS"))
}
