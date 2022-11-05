use crate::prelude::*;
use base_engine::prelude::*;

use polars::lazy::dsl::apply_multiple;

pub(crate) fn drc_secnonctp_grossjtd(_: &OCP) -> Expr {
    rc_sens("DRC_SecNonCTP", col("GrossJTD"))
}
pub(crate) fn drc_secnonctp_grossjtd_scaled(_: &OCP) -> Expr {
    rc_sens("DRC_SecNonCTP", col("GrossJTD") * col("ScaleFactor"))
}

pub(crate) fn drc_secnonctp_charge(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::CapitalCharge)
}
pub(crate) fn drc_secnonctp_netlongjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::NetLongJTD)
}
pub(crate) fn drc_secnonctp_netshortjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::NetShortJTD)
}
pub(crate) fn drc_secnonctp_weightednetlongjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetLongJTD)
}
pub(crate) fn drc_secnonctp_weightednetabsshortjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetAbsShortJTD)
}
pub(crate) fn drc_secnonctp_hbr(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::Hbr)
}

fn drc_secnonctp_distributor(_: &OCP, rtrn: ReturnMetric) -> Expr {
    drc_secnonctp_charge_calculator(rtrn)
}

/// DRC Sec Non CTP Offsetting (22.30) is not implemented yet
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
            // First, sum over bucket, obligor and seniority
            let mut lf = df
                .lazy()
                .filter(col("rc").eq(lit("DRC_SecNonCTP")))
                .groupby([col("b"), col("rf"), col("rft"), col("tr")])
                .agg([
                    (col("jtd") * col("s")).sum().alias("scaled_jtd"),
                    col("w").first(),
                ]);

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
                            |x| Ok(x.f64()?.apply(|y| y.abs()).into_series()),
                            GetOutput::from_type(DataType::Float64),
                        )
                        .alias("NetAbsShortJTD"),
                );
            // Apply Weights
            df = lf.collect()?;
            let res_len = columns[0].len();
            match rtrn {
                ReturnMetric::NetLongJTD => {
                    return Ok(Series::from_vec(
                        "Res",
                        vec![df["NetLongJTD"].sum::<f64>().unwrap_or_default(); res_len],
                    )
                    .into_series())
                }
                ReturnMetric::NetShortJTD => {
                    return Ok(Series::from_vec(
                        "Res",
                        vec![df["NetShortJTD"].sum::<f64>().unwrap_or_default(); res_len],
                    )
                    .into_series())
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
                ReturnMetric::Hbr => Ok(Series::from_vec(
                    "Res",
                    vec![df["HBR"].sum::<f64>().unwrap_or_default(); res_len],
                )
                .into_series()),
                ReturnMetric::WeightedNetLongJTD => Ok(Series::from_vec(
                    "Res",
                    vec![df["WeightedNetLongJTD"].sum::<f64>().unwrap_or_default(); res_len],
                )
                .into_series()),
                ReturnMetric::WeightedNetAbsShortJTD => Ok(Series::from_vec(
                    "Res",
                    vec![
                        df["WeightedNetAbsShortJTD"]
                            .sum::<f64>()
                            .unwrap_or_default();
                        res_len
                    ],
                )
                .into_series()),
                _ => Ok(Float64Chunked::from_vec(
                    "Res",
                    vec![df["DRCBucket"].sum::<f64>().unwrap_or_default(); res_len],
                )
                .into_series()),
            }
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("RiskFactor"),
            col("RiskFactorType"), //Seniority
            col("Tranche"),
            col("GrossJTD"),
            col("SensWeights").arr().get(lit(0)),
            col("ScaleFactor"),
        ],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

pub(crate) fn drc_secnonctp_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "DRC_SecNonCTP_GrossJTD".to_string(),
            calculator: Box::new(drc_secnonctp_grossjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_GrossJTD_Scaled".to_string(),
            calculator: Box::new(drc_secnonctp_grossjtd_scaled),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_CapitalCharge".to_string(),
            calculator: Box::new(drc_secnonctp_charge),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_NetLongJTD".to_string(),
            calculator: Box::new(drc_secnonctp_netlongjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_NetShortJTD".to_string(),
            calculator: Box::new(drc_secnonctp_netshortjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_NetLongJTD_Weighted".to_string(),
            calculator: Box::new(drc_secnonctp_weightednetlongjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_NetShortJTD_Weighted".to_string(),
            calculator: Box::new(drc_secnonctp_weightednetabsshortjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
        Measure {
            name: "DRC_SecNonCTP_HBR".to_string(),
            calculator: Box::new(drc_secnonctp_hbr),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_SecNonCTP"))),
        },
    ]
}
