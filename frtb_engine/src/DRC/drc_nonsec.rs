use crate::prelude::*;
use base_engine::prelude::*;

//use ndarray::prelude::*;
use polars::prelude::*;
use once_cell::sync::Lazy;


pub(crate) fn drc_nonsec_grossjtd(_: &OCP) -> Expr {
    rc_sens("DRC_NonSec", col("GrossJTD"))
}
pub(crate) fn drc_nonsec_grossjtd_scaled(_: &OCP) -> Expr {
    //drc_nonsec_distributor(op, ReturnMetric::ScaledGrossJTD)
    rc_sens("DRC_NonSec", col("GrossJTD")*col("ScaleFactor"))
}

pub(crate) fn drc_nonsec_charge(op: &OCP) -> Expr {
    drc_nonsec_distributor(op, ReturnMetric::CapitalCharge)
}

fn drc_nonsec_distributor(
    _: &OCP,
    rtrn: ReturnMetric,
) -> Expr {
    drc_nonsec_charge_calculator(rtrn)
}

/// calculate FX Delta Capital charge
fn drc_nonsec_charge_calculator(
    rtrn: ReturnMetric,
) -> Expr
{
    // inner function
    apply_multiple(
        move |columns| {
            let mut df = df![
                "rc"   => &columns[0],
                "b"    => &columns[1],
                "rf"   => &columns[2],
                "rft"  => &columns[3],
                "jtd"  => &columns[4],
                "w"    => &columns[5],
                "s"    => &columns[6],
            ]?;

            df = df
                .lazy()
                .filter(
                    col("rc").eq(lit("DRC_NonSec"))
                )
                .groupby([col("b"), col("rf"), col("rft")])
                .agg([
                    (col("jtd")*col("s")).sum().alias("scaled_jtd"),
                    col("w").first(),
                ])
                .collect()?;
            
            dbg!(&df);
            let res_len = columns[0].len();
            //match rtrn {
            //    ReturnMetric::ScaledGrossJTD => {
            //        return Ok(
            //            Float64Chunked::from_vec("Res", vec![df["scaled_jtd"].sum().unwrap_or_default(); res_len])
            //                .into_series(),
            //        )
            //    },
            //    _ => (),
            //};

            return Ok(
                Float64Chunked::from_vec("Res", vec![0.; res_len])
                    .into_series(),
            )
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("RiskFactor"),
            col("RiskFactorType"),
            col("GrossJTD"),
            col("SensWeights").arr().get(0),
            col("ScaleFactor"),
        ],
        GetOutput::from_type(DataType::Float64),
    )
}

pub(crate) fn drc_nonsec_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "DRC_NonSec_GrossJTD".to_string(),
            calculator: Box::new(drc_nonsec_grossjtd),
            aggregation: None,
            precomputefilter: Some(
                col("RiskClass")
                    .eq(lit("DRC_NonSec"))
            ),
        },
        Measure {
            name: "DRC_NonSec_GrossJTD_Scaled".to_string(),
            calculator: Box::new(drc_nonsec_grossjtd_scaled),
            aggregation: None,
            precomputefilter: Some(
                col("RiskClass")
                    .eq(lit("DRC_NonSec"))
            ),
        },
        Measure {
            name: "DRC_NonSec_CapitalCharge".to_string(),
            calculator: Box::new(drc_nonsec_charge),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskClass")
                    .eq(lit("DRC_NonSec"))
            ),
        },
    ]
}

pub static DRC_SENIORITY: Lazy<HashMap<&str, u8>> =
    Lazy::new(||HashMap::from(
        [
            ("Covered",         4),
            ("Senior",          3),
            ("SeniorSecured",   2),
            ("SeniorUnsecured", 1),
            ("Equity",          0)
        ]
    ));