use crate::prelude::*;
use ultibi::{
    polars::prelude::{apply_multiple, df, ChunkApply, DataType, GetOutput, IntoSeries},
    BaseMeasure, IntoLazy, CPM,
};

//use polars::lazy::dsl::apply_multiple;

pub(crate) fn drc_secnonctp_grossjtd(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_sens("DRC_Sec_nonCTP", col("GrossJTD")))
}
pub(crate) fn drc_secnonctp_grossjtd_scaled(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_sens(
        "DRC_Sec_nonCTP",
        col("GrossJTD") * col("ScaleFactor"),
    ))
}

pub(crate) fn drc_secnonctp_charge(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::CapitalCharge)
}
pub(crate) fn drc_secnonctp_netlongjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::NetLongJTD)
}
pub(crate) fn drc_secnonctp_netshortjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::NetShortJTD)
}
pub(crate) fn drc_secnonctp_weightednetlongjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetLongJTD)
}
pub(crate) fn drc_secnonctp_weightednetabsshortjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetAbsShortJTD)
}
pub(crate) fn drc_secnonctp_hbr(op: &CPM) -> PolarsResult<Expr> {
    drc_secnonctp_distributor(op, ReturnMetric::Hbr)
}

fn drc_secnonctp_distributor(_: &CPM, rtrn: ReturnMetric) -> PolarsResult<Expr> {
    Ok(drc_secnonctp_charge_calculator(rtrn))
}

/// TODO DRC Sec Non CTP Offsetting (22.30) is not implemented yet
fn drc_secnonctp_charge_calculator(rtrn: ReturnMetric) -> Expr {
    // inner function
    apply_multiple(
        move |columns| {
            let mut df = df![
                "rc"   => &columns[0],
                "b"    => &columns[1],
                "rf"   => &columns[2],
                "rft"  => &columns[3],
                "tr"   => &columns[4],
                "jtd"  => &columns[5],
                "w"    => &columns[6],
                "s"    => &columns[7],
            ]?;

            // Safety Step
            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            // First, sum over bucket, obligor and seniority
            let mut lf = df
                .lazy()
                .filter(col("rc").eq(lit("DRC_Sec_nonCTP")))
                .groupby([col("b"), col("rf"), col("rft"), col("tr")])
                .agg([
                    (col("jtd") * col("s")).sum().alias("scaled_jtd"),
                    col("w").first(),
                ]);

            // Safety step
            df = lf.collect()?;
            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };
            lf = df.lazy();

            // TODO  22.30

            // Split Scaled GrossJTD into NetLong and NetShort
            df = lf.collect()?;
            lf = df
                .lazy()
                .with_columns([
                    when(col("scaled_jtd").gt(lit::<f64>(0.)))
                        .then(col("scaled_jtd"))
                        .otherwise(NULL.lit())
                        .alias("NetLongJTD"),
                    when(col("scaled_jtd").lt(lit::<f64>(0.)))
                        .then(col("scaled_jtd"))
                        .otherwise(NULL.lit())
                        .alias("NetShortJTD"),
                ])
                .with_column(
                    col("NetShortJTD")
                        .map(
                            |x| Ok(Some(x.f64()?.apply(|y| y.abs()).into_series())),
                            GetOutput::from_type(DataType::Float64),
                        )
                        .alias("NetAbsShortJTD"),
                );
            // Apply Weights
            df = lf.collect()?;

            match rtrn {
                ReturnMetric::NetLongJTD => {
                    return Ok(Some(Series::new(
                        "Res",
                        [df["NetLongJTD"].sum::<f64>().unwrap_or_default()],
                    )))
                }
                ReturnMetric::NetShortJTD => {
                    return Ok(Some(Series::new(
                        "Res",
                        [df["NetShortJTD"].sum::<f64>().unwrap_or_default()],
                    )))
                }
                _ => (),
            };
            lf = df
                .lazy()
                .groupby([col("b")])
                .agg([
                    col("NetLongJTD").sum(),
                    col("NetShortJTD").sum(),
                    col("NetAbsShortJTD").sum(),
                    (col("NetLongJTD") * col("w"))
                        .sum()
                        .alias("WeightedNetLongJTD"),
                    (col("NetAbsShortJTD") * col("w"))
                        .sum()
                        .alias("WeightedNetAbsShortJTD"),
                ])
                .fill_null(lit::<f64>(0.))
                .with_column(
                    when((col("NetLongJTD") + col("NetAbsShortJTD")).neq(lit::<f64>(0.)))
                        .then(col("NetLongJTD") / (col("NetLongJTD") + col("NetAbsShortJTD")))
                        .otherwise(lit::<f64>(0.))
                        .alias("HBR"),
                )
                .with_column(
                    (col("WeightedNetLongJTD") - col("WeightedNetAbsShortJTD") * col("HBR"))
                        .alias("DRCBucket"),
                );
            df = lf.collect()?;

            match rtrn {
                ReturnMetric::Hbr => Ok(Some(Series::new(
                    "Res",
                    [df["HBR"].sum::<f64>().unwrap_or_default()],
                ))),
                ReturnMetric::WeightedNetLongJTD => Ok(Some(Series::new(
                    "Res",
                    [df["WeightedNetLongJTD"].sum::<f64>().unwrap_or_default()],
                ))),
                ReturnMetric::WeightedNetAbsShortJTD => Ok(Some(Series::new(
                    "Res",
                    [df["WeightedNetAbsShortJTD"]
                        .sum::<f64>()
                        .unwrap_or_default()],
                ))),
                _ => Ok(Some(Series::new(
                    "Res",
                    [df["DRCBucket"].sum::<f64>().unwrap_or_default()],
                ))),
            }
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("RiskFactor"),
            col("RiskFactorType"), //Seniority
            col("Tranche"),
            col("GrossJTD"),
            col("SensWeights").list().get(lit(0)),
            col("ScaleFactor"),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

pub(crate) fn drc_secnonctp_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP GrossJTD".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_grossjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP GrossJTD Scaled".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_grossjtd_scaled),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP CapitalCharge".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_charge),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP NetLongJTD".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_netlongjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP NetShortJTD".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_netshortjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP NetLongJTD Weighted".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_weightednetlongjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP NetShortJTD Weighted".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_weightednetabsshortjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_Sec_nonCTP"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC Sec nonCTP HBR".to_string(),
            calculator: std::sync::Arc::new(drc_secnonctp_hbr),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
            calc_params: vec![],
        }),
    ]
}
