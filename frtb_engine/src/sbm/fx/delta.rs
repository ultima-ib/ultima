//! For FX RiskFactor is the original source of risk, could be offshore
//! BucketBCBS/CRR2 to be

use ndarray::{Array, Array1};

use crate::{
    helpers::get_jurisdiction,
    prelude::*,
    sbm::common::{across_bucket_agg, SBMChargeType},
};
use polars::lazy::dsl::apply_multiple;

/// This works for cases like GBP reporting with BCBS params
pub(crate) fn ccy_regex(op: &OCP) -> String {
    let juri: Jurisdiction = get_jurisdiction(op);
    op.get("reporting_ccy")
        .and_then(|s| {
            if s.len() == 3 {
                Some(format!("^...{s}$"))
            } else {
                None
            }
        })
        .unwrap_or_else(|| match juri {
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => "^...EUR$".to_string(),
            _ => "^...USD$".to_string(),
        })
}

/// Returns a Series equal to SensitivitySpot with RiskClass == FX and RiskFactor == ...CCY
/// !where CCY is either provided as part of optional parameters,
/// !and if not, then is based on Jurisdiction
pub(crate) fn fx_delta_sens_repccy(op: &OCP) -> Expr {
    let ccy_regex = ccy_regex(op);

    apply_multiple(
        move |columns| {
            let mask1 = columns[0].utf8()?.equal("FX");

            // function to take rep_ccy as an argument
            let mask2 = columns[1].utf8()?.contains(ccy_regex.as_str())?;

            // function to take rep_ccy as an argument
            let mask3 = columns[3].utf8()?.equal("Delta");

            // Set delta's which don't match mask1 or mask2 to None (ie NaN)
            let delta = columns[2].f64()?.set(&!(mask1 & mask2 & mask3), None)?;

            Ok(delta.into_series())
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("SensitivitySpot"),
            col("RiskCategory"),
        ],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

/// takes CalcParams because we need to know reporting CCY
pub(crate) fn fx_delta_sens_weighted(op: &OCP) -> Expr {
    fx_delta_sens_repccy(op) * col("SensWeights").arr().get(lit(0))
}
///calculate FX Delta Sb, same for all scenarios
pub(crate) fn fx_delta_sb(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

///calculate FX Delta Kb, same for all scenarios
pub(crate) fn fx_delta_kb(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate FX Delta High Capital charge
pub(crate) fn fx_delta_charge_high(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate FX Delta Medium Capital charge
pub(crate) fn fx_delta_charge_medium(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate FX Delta Low Capital charge
pub(crate) fn fx_delta_charge_low(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn fx_delta_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    //let fx_delta_sens_weighted_with_rep_ccy = fx_delta_sens_weighted(op);
    let ccy_regex = ccy_regex(op);

    let _suffix = scenario.as_str();

    let fx_delta_gamma = get_optional_parameter(
        op,
        format!("fx_delta_gamma{_suffix}").as_ref() as &str,
        &scenario.fx_delta_vega_gamma,
    );

    fx_delta_charge(fx_delta_gamma, rtrn, ccy_regex)
}

/// calculate FX Delta Capital charge
/// Note, we don't want to run a regex on the whole column
/// Hence it makes sence to run regex after filtering
fn fx_delta_charge(gamma: f64, rtrn: ReturnMetric, ccy_regex: String) -> Expr {
    // inner function
    apply_multiple(
        move |columns| {
            let df = df![
                "rcat" => &columns[0],
                "rc"   => &columns[1],
                "b"    => &columns[2],
                "d"    => &columns[3],
                "w"    => &columns[4],
            ]?;

            let ccy_regex = ccy_regex.clone();
            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit("FX"))
                        .and(col("rcat").eq(lit("Delta")))
                        .and(col("b").apply(
                            move |col| Ok(col.utf8()?.contains(&ccy_regex)?.into_series()),
                            GetOutput::from_type(DataType::Boolean),
                        )),
                )
                .groupby([col("b")])
                .agg([(col("d") * col("w")).sum().alias("dw_sum")])
                // Drop nulls (ie other reporting ccys)
                .drop_nulls(Some(vec![col("dw_sum")]))
                .collect()?;

            if df.height() == 0 {
                return Ok(Series::new("res", [0.]));
            };

            //21.4.4 |dw_sum| == kb for FX
            //21.4.5.a sb == dw_sum
            let dw_sum = df["dw_sum"].f64()?.to_ndarray()?; //Ok since we have filtered out NULLs above
                                                            // Early return Kb or Sb, ie the required metric
            let res_len = columns[0].len();
            if let ReturnMetric::Sb = rtrn {
                return Ok(Series::new("res", [dw_sum.sum()]));
            }

            let kbs: Array1<f64> = dw_sum.iter().map(|x| x.abs()).collect();
            if let ReturnMetric::Kb = rtrn {
                return Ok(Series::new("res", [kbs.sum()]));
            }

            let mut gamma = Array::from_elem((dw_sum.len(), dw_sum.len()), gamma);
            let zeros = Array::zeros(dw_sum.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(
                kbs,
                dw_sum.to_owned(),
                &gamma,
                res_len,
                SBMChargeType::DeltaVega,
            )
        },
        &[
            col("RiskCategory"),
            col("RiskClass"),
            col("BucketBCBS"),
            col("SensitivitySpot"),
            col("SensWeights").arr().get(lit(0)),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}
/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn fx_delta_max(op: &OCP) -> Expr {
    max_exprs(&[
        fx_delta_charge_low(op),
        fx_delta_charge_medium(op),
        fx_delta_charge_high(op),
    ])
}
/// Exporting Measures
pub(crate) fn fx_delta_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "FX DeltaSens".to_string(),
            calculator: Box::new(fx_delta_sens_repccy),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaSens Weighted".to_string(),
            calculator: Box::new(fx_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaSb".to_string(),
            calculator: Box::new(fx_delta_sb),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaKb".to_string(),
            calculator: Box::new(fx_delta_kb),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaCharge Low".to_string(),
            calculator: Box::new(fx_delta_charge_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaCharge Medium".to_string(),
            calculator: Box::new(fx_delta_charge_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaCharge High".to_string(),
            calculator: Box::new(fx_delta_charge_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
        Measure {
            name: "FX DeltaCharge MAX".to_string(),
            calculator: Box::new(fx_delta_max),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
        },
    ]
}
