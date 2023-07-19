use crate::prelude::*;
use ultibi::{
    polars::prelude::{
        apply_multiple, df, ChunkApply, DataType, GetOutput, IntoSeries, NamedFromOwned,
    },
    BaseMeasure, IntoLazy, CPM,
};

//use polars::{lazy::dsl::apply_multiple, prelude::NamedFromOwned};
//use polars::prelude::ChunkApply;

pub(crate) fn drc_nonsec_grossjtd(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_sens("DRC_nonSec", col("GrossJTD")))
}
pub(crate) fn drc_nonsec_grossjtd_scaled(_: &CPM) -> PolarsResult<Expr> {
    //drc_nonsec_distributor(op, ReturnMetric::ScaledGrossJTD)
    Ok(rc_sens("DRC_nonSec", col("GrossJTD") * col("ScaleFactor")))
}

pub(crate) fn drc_nonsec_charge(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::CapitalCharge)
}
pub(crate) fn drc_nonsec_netlongjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::NetLongJTD)
}
pub(crate) fn drc_nonsec_netshortjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::NetShortJTD)
}
pub(crate) fn drc_nonsec_weightednetlongjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::WeightedNetLongJTD)
}
pub(crate) fn drc_nonsec_weightednetabsshortjtd(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::WeightedNetAbsShortJTD)
}
pub(crate) fn drc_nonsec_hbr(op: &CPM) -> PolarsResult<Expr> {
    drc_nonsec_distributor(op, ReturnMetric::Hbr)
}

fn drc_nonsec_distributor(op: &CPM, rtrn: ReturnMetric) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;
    let offset = get_optional_parameter(op, "drc_offset", &true)?;

    let weights = match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => col("SensWeightsCRR2"),
        _ => col("SensWeights"),
    };
    Ok(drc_nonsec_charge_calculator(rtrn, offset, weights))
}

/// calculate FX Delta Capital charge
fn drc_nonsec_charge_calculator(rtrn: ReturnMetric, offset: bool, weights: Expr) -> Expr {
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

            // Safety Step
            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            // First, sum over bucket, obligor and seniority
            let mut lf = df
                .lazy()
                .filter(col("rc").eq(lit("DRC_nonSec")))
                .groupby([col("b"), col("rf"), col("rft")])
                .agg([
                    (col("jtd") * col("s")).sum().alias("scaled_jtd"),
                    col("w").first(),
                ]);

            let schema = lf.schema()?;

            // Safety step
            df = lf.collect()?;
            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };
            lf = df.lazy();

            // Do you want to aggregate as per  22.19?
            // Note, the algorithm is O(N), but we loose Negative GrossJTD position changes,
            // (and might end up with a different Credit Quality, but same obligor)
            // This shouldn't be a problem since we sum positions (netShort netLong) anyway,
            // And THEN apply CreditQuality weights, BECAUSE Obligor - CreditQuality should be 1to1 map
            if offset {
                lf = lf
                    .sort_by_exprs(&[col("rft")], [false], false, true)
                    .groupby(["b", "rf"])
                    .apply(
                        |mut df| {
                            let mut neg = 0.;
                            let mut neg_flag = false; //flags if we have any negative values
                            let mut res: Vec<f64> = Vec::with_capacity(df["scaled_jtd"].len());
                            df["scaled_jtd"].f64()?.into_no_null_iter().for_each(|x| {
                                if x < 0. {
                                    neg += x;
                                    neg_flag = true;
                                } else {
                                    let diff = x + neg;
                                    if diff < 0. {
                                        res.push(0.);
                                        neg = diff;
                                    } else {
                                        res.push(diff);
                                        neg = 0.
                                    }
                                }
                            });
                            if neg_flag {
                                res.push(neg);
                                for _ in 0..(df["scaled_jtd"].len() - res.len()) {
                                    res.push(0.)
                                }
                            }
                            df.with_column(Series::from_vec("scaled_jtd", res))?;
                            Ok(df)
                        },
                        schema,
                    );
            };
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
            col("SeniorityRank"),
            col("GrossJTD"),
            weights.list().get(lit(0)),
            col("ScaleFactor"),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

pub(crate) fn drc_nonsec_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "DRC nonSec GrossJTD".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_grossjtd),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec GrossJTD Scaled".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_grossjtd_scaled),
            aggregation: None,
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec CapitalCharge".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_charge),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec NetLongJTD".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_netlongjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec NetShortJTD".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_netshortjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec NetLongJTD Weighted".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_weightednetlongjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "DRC nonSec NetAbsShortJTD Weighted".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_weightednetabsshortjtd),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
        // HBR Only makes sence at Bucket level
        Measure::Base(BaseMeasure {
            name: "DRC nonSec HBR".to_string(),
            calculator: std::sync::Arc::new(drc_nonsec_hbr),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(col("RiskClass").eq(lit("DRC_nonSec"))),
            calc_params: vec![],
        }),
    ]
}
