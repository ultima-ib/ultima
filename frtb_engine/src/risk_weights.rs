//! This module includes complete weights allocation logic (pre build mode). SBM and DRC weights and Scale Factors
//! 
//! TODO move all risk weights to a .csv/.json file and join/lookup. This would give flexibility with adjusting RiskWeights upon startup.

use polars::prelude::*;
use std::collections::HashMap;
use crate::drc::drc_weights;

/// Calls .utf8() on `c` and iterates over map matching regex  
/// 
/// Potential optimisation to use &'static str instead of String (it has to be 'static due to apply)
fn rf_rw_map(c: Expr, map: HashMap<String, Expr>, other: Expr) -> Expr {
    // buf is a placeholder
    let mut it = map.into_iter();
    let (k, v) = it.next().unwrap(); //The map will have at least one value

    let mut buf = when(lit::<bool>(false))
        .then(lit::<f64>(0.).list())
        .when(c.clone().apply(
            move |s|{ 
            Ok(s.utf8()?.contains(k.as_str())?.into_series())},
            GetOutput::from_type(DataType::Boolean),
        ))
        .then(v);

    for (k, v) in it {
        buf = buf
            .when(c.clone().apply(
                move |s| Ok(s.utf8()?.contains(k.as_str())?.into_series()),
                GetOutput::from_type(DataType::Boolean),
            ))
            .then(v);
    }
    buf.otherwise(other)
}

pub fn weight_assign_logic(weights: SensWeightsConfig) -> Expr {
    let x: Option<f64> = None;
    let not_yet_implemented = Series::new("null", &[x]).lit().list();
    let never_reached = Series::new("null", &[x]).lit().list();
    when(col("RiskCategory").eq(lit("Delta")))
        .then(
            // FX
            when(col("RiskClass").eq(lit("FX")))
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.fx_override,
                    weights.fx,
                ))
                //GIRR
                .when(
                    col("RiskClass").eq(lit("GIRR")).and(
                        col("RiskFactorType")
                            .eq(lit("XCCY"))
                            .or(col("RiskFactorType").eq(lit("Inflation"))),
                    ),
                )
                .then(
                    //temp shortcut, since xccy weight = infl weight
                    weights.ir_xccy_infl,
                )
                .when(
                    col("RiskClass")
                        .eq(lit("GIRR"))
                        .and(col("RiskFactorType").eq(lit("Yield"))),
                )
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.ir_override,
                    weights.ir_yield,
                ))
                // Commodity
                .when(col("RiskClass").eq(lit("Commodity")))
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.com_bucket_weight,
                    never_reached.clone(),
                ))
                // Equity
                .when(
                    col("RiskClass")
                        .eq(lit("Equity"))
                        .and(col("RiskFactorType").eq(lit("EqSpot"))),
                )
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.eq_bucket_spot_weight,
                    never_reached.clone(),
                ))
                .when(
                    col("RiskClass")
                        .eq(lit("Equity"))
                        .and(col("RiskFactorType").eq(lit("EqRepo"))),
                )
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.eq_bucket_repo_weight,
                    never_reached.clone(),
                ))
                // CSR non-Sec
                .when(col("RiskClass").eq(lit("CSR_nonSec")))
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.csr_non_sec_weight,
                    never_reached.clone(),
                ))
                // CSR secCTP
                .when(col("RiskClass").eq(lit("CSR_Sec_CTP")))
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.csr_sec_ctp_weight,
                    never_reached.clone(),
                ))
                // CSR sec nonCTP
                .when(col("RiskClass").eq(lit("CSR_Sec_nonCTP")))
                .then(rf_rw_map(
                    col("BucketBCBS"),
                    weights.csr_sec_nonctp_weight,
                    never_reached.clone(),
                ))
                .otherwise(not_yet_implemented.clone()),
        )
    .when(col("RiskCategory").eq(lit("Vega")))
    .then(
        when(col("RiskClass").neq(lit("Equity"))) //all vega except Equity
            .then(rf_rw_map(
                col("RiskClass"),
                weights.vega_risk_class_weight,
                never_reached.clone(),
            ))
            // Equity, special case
            .otherwise(rf_rw_map(
                col("BucketBCBS"),
                weights.vega_equity_weight,
                never_reached.clone(),
            )),
    )
    .when(col("RiskCategory").eq(lit("DRC")))
    .then(
        when(col("RiskClass").eq(lit("DRC_NonSec")))
        .then(rf_rw_map(
            col("CreditQuality"),
            weights.drc_nonsec,
            never_reached.clone(),
        ))
        // DRC Sec Non-CTP Risk Weight
        //.when(col("RiskClass").eq(lit("DRC_SecNonCTP")))
        //.then(rf_rw_map(
        //    concat_str([
        //        col("CreditQuality").map(|s|Ok(s.utf8()?.to_uppercase().into_series()), GetOutput::from_type(DataType::Utf8)), 
        //        col("RiskFactorType").map(|s|Ok(s.utf8()?.to_uppercase().into_series()), GetOutput::from_type(DataType::Utf8)),
        //        ], 
        //        "_"),
        //    weights.drc_secnonctp,
        //    never_reached,
        //))
        .otherwise(not_yet_implemented.clone()))
    .otherwise(not_yet_implemented)
}

/// Default Risk Weights as per regulation are defined here
pub fn weights_assign(conf: &HashMap<String, String>) -> Expr {
    // FX
    //21.88 - Conservative, false by default
    let fx_sqrt2_div = conf
        .get("fx_sqrt2_div")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or_else(||false);
    let fx_1_over_sqrt2 = if fx_sqrt2_div {
        1. / 2.0_f64.sqrt()
    } else {
        1_f64
    };

    let fx_base = &[0.15];
    let fx_base_srs = Series::new("", fx_base);

    // Order is important, later will override previous
    let fx_map = HashMap::from([
        ("HRKEUR|BGNEUR".to_string(),   Series::new("",&[0.05]).lit().list() ),
        ("DKKEUR".to_string(),          Series::new("",&[0.0225]).lit().list() ),
        ("^(USD|EUR|JPY|GBP|AUD|CAD|CHF|MXN|CNY|CNO|NZD|RUB|HKD|SGD|TRY|KRW|SEK|ZAR|INR|NOK|BRL|DKK)...$".to_string(), 
                                        (Series::new("", fx_base) * fx_1_over_sqrt2).lit().list() ),
        ("USDUSD|EUREUR".to_string(),   Series::new("",&[0.]).lit().list() ),
    ]);

    // GIRR
    //21.44 - Conservative, false by default
    let girr_sqrt2_div = conf
        .get("girr_sqrt2_div")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);
    let girr_1_over_sqrt2 = if girr_sqrt2_div {
        1. / 2.0_f64.sqrt()
    } else {
        1_f64
    };
    let ir_base = &[
        0., 0.017, 0.017, 0.016, 0.013, 0.012, 0.011, 0.011, 0.011, 0.011, 0.011,
    ];
    let ir_base_srs = Series::new("", ir_base);

    let ir_map = HashMap::from([(
        "EUR|USD|GBP|AUD|JPY|SEK|CAD".to_string(),
        (Series::new("", ir_base) * girr_1_over_sqrt2).lit().list(),
    )]);
    let ir_xccy = Series::new("", &[0.016]);
    //let ir_infl = ir_xccy.clone(); <--- not needed atm as xccy weight == infl weight

    // Commodity
    // 21.82
    // Commodity risk is defined for 11 tenors
    let commodity_weights = [0.3, 0.35, 0.6, 0.8, 0.4, 0.45, 0.2, 0.35, 0.25, 0.35, 0.5];
    let commodity_bucket_weight: HashMap<String, Expr> = bucket_weight_map(&commodity_weights);

    // Equity
    // 21.77
    let equity_spot_weights = [
        0.55, 0.6, 0.45, 0.55, 0.3, 0.35, 0.4, 0.5, 0.7, 0.5, 0.7, 0.15, 0.25,
    ];
    let equity_bucket_spot_weights: HashMap<String, Expr> = bucket_weight_map(&equity_spot_weights);

    let equity_repo_weights = [
        0.0055, 0.006, 0.0045, 0.0055, 0.003, 0.0035, 0.004, 0.005, 0.007, 0.005, 0.007, 0.0015,
        0.0025,
    ];
    let equity_bucket_repo_weights: HashMap<String, Expr> = bucket_weight_map(&equity_repo_weights);

    // CSR nonsec
    // 21.53
    // same weight for all tenors, hence we simplify by keeping just one
    // TODO when arr gets fixed expand to all 5 tenors in order to use Total Delta/Total Weighted Delta
    let csr_nonsec_weights_arr = [
        0.005, 0.01, 0.05, 0.03, 0.03, 0.02, 0.015, 0.025, 0.02, 0.04, 0.12, 0.07, 0.085, 0.055,
        0.05, 0.12, 0.015, 0.05,
    ];
    let csr_non_sec_weight: HashMap<String, Expr> = bucket_weight_map(&csr_nonsec_weights_arr);

    // CSR Sec CTP 21.59
    let csr_sec_ctp_weights_arr = [
        0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12, 0.12, 0.12,
        0.13,
    ];
    let csr_sec_ctp_weight: HashMap<String, Expr> = bucket_weight_map(&csr_sec_ctp_weights_arr);

    // CSR Sec nonCTP 21.62 and 325am
    let csr_sec_nonctp_weight_arr: [f64; 25] = [
        0.009, 0.015, 0.02, 0.02, 0.008, 0.012, 0.012, 0.014, 0.0125, 0.01875, 0.025, 0.025, 0.01,
        0.015, 0.015, 0.0175, 0.01575, 0.02625, 0.035, 0.035, 0.014, 0.021, 0.021, 0.0245, 0.035,
    ];
    let csr_sec_nonctp_weight: HashMap<String, Expr> =
        bucket_weight_map(&csr_sec_nonctp_weight_arr);

    fn vega_rw(lh: u8) -> f64 {
        (0.55 * (lh as f64).sqrt() / (10f64).sqrt()).min(1.)
    }

    let vega_risk_class_weight: HashMap<String, Expr> = HashMap::from([
        (
            "^GIRR$".to_string(),
            Series::from_vec("", vec![vega_rw(60); 1]).lit().list(),
        ),
        (
            "^CSR_nonSec$".to_string(),
            Series::from_vec("", vec![vega_rw(120); 1]).lit().list(),
        ),
        (
            "^CSR_Sec_CTP$".to_string(),
            Series::from_vec("", vec![vega_rw(120); 1]).lit().list(),
        ),
        (
            "^CSR_Sec_nonCTP$".to_string(),
            Series::from_vec("", vec![vega_rw(120); 1]).lit().list(),
        ),
        //("^Equity$".to_string(), vega_rw(60)), // Equity small cap
        (
            "^Commodity$".to_string(),
            Series::from_vec("", vec![vega_rw(120); 1]).lit().list(),
        ),
        (
            "^FX$".to_string(),
            Series::from_vec("", vec![vega_rw(40); 1]).lit().list(),
        ),
    ]);
    let eq_large_cap = vega_rw(20);
    let eq_small_cap = vega_rw(60);
    let equity_vega_weights = [
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_large_cap,
        eq_small_cap,
        eq_small_cap,
        eq_small_cap,
        eq_large_cap,
        eq_large_cap,
    ];
    let vega_equity_weight: HashMap<String, Expr> = bucket_weight_map(&equity_vega_weights);

    let drc_nonsec = drc_weights::drc_nonsec_weights();

    let drc_secnonctp = drc_weights::drc_secnonctp_weights();

    let dlt_weights = SensWeightsConfig {
        // FX
        fx: fx_base_srs.lit().list(),
        fx_override: fx_map,
        // GIRR
        ir_xccy_infl: ir_xccy.lit().list(),
        ir_yield: ir_base_srs.lit().list(),
        //ir_infl: ir_infl.lit().list(),
        ir_override: ir_map,
        // Commodity
        com_bucket_weight: commodity_bucket_weight,
        // Eq
        eq_bucket_spot_weight: equity_bucket_spot_weights,
        eq_bucket_repo_weight: equity_bucket_repo_weights,
        // CSR non-Sec
        csr_non_sec_weight,
        // CSR sec CTP
        csr_sec_ctp_weight,
        // CSR sec nonCTP
        csr_sec_nonctp_weight,
        // Vega RW except Eq
        vega_risk_class_weight,
        // Vega Eq
        vega_equity_weight,
        // DRC Non Sec
        drc_nonsec,
        //DRC Sec NonCTP
        drc_secnonctp
    };

    //Assign Delta Weights
    weight_assign_logic(dlt_weights)
}

pub struct SensWeightsConfig {
    // FX
    fx: Expr,
    fx_override: HashMap<String, Expr>,
    // GIRR
    ir_xccy_infl: Expr, //temp shortcut since infl and xccy weight is same
    ir_yield: Expr,
    ir_override: HashMap<String, Expr>,
    // Commodity
    com_bucket_weight: HashMap<String, Expr>,
    // Equity
    eq_bucket_spot_weight: HashMap<String, Expr>,
    eq_bucket_repo_weight: HashMap<String, Expr>,
    // CSR non-Sec
    csr_non_sec_weight: HashMap<String, Expr>,
    // CSR sec CTP
    csr_sec_ctp_weight: HashMap<String, Expr>,
    // CSR sec nonCTP
    csr_sec_nonctp_weight: HashMap<String, Expr>,

    //Vega Risk Class except Equity
    vega_risk_class_weight: HashMap<String, Expr>,

    //Vega Equity
    vega_equity_weight: HashMap<String, Expr>,

    //DRC Non Sec
    drc_nonsec: HashMap<String, Expr>,
    //DRC Sec NonCTP
    drc_secnonctp: HashMap<String, Expr>,
}

/// Ammends BCBS risk weights into CRR2 compliance where required
#[cfg(feature = "CRR2")]
pub fn weights_assign_crr2() -> Expr {
    let x: Option<f64> = None;
    //let not_yet_implemented = Series::new("null", &[x]).lit().list();
    let never_reached = Series::new("null", &[x]).lit().list();

    let csr_nonsec_weights = [
        0.005, 0.005, 0.01, 0.05, 0.03, 0.03, 0.02, 0.015, 0.1, 0.025, 0.02, 0.04, 0.12, 0.07,
        0.085, 0.055, 0.05, 0.12, 0.015, 0.05,
    ];
    let csr_non_sec_weight_crr2: HashMap<String, Expr> = bucket_weight_map(&csr_nonsec_weights);

    // CSR Sec CTP 21.59
    let csr_sec_ctp_weights_crr2 = [
        0.04, 0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.03, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12,
        0.12, 0.12, 0.13,
    ];
    let csr_sec_ctp_weight_crr2: HashMap<String, Expr> =
        bucket_weight_map(&csr_sec_ctp_weights_crr2);

    let drc_nonsec_crr2 = drc_weights::drc_nonsec_weights_crr2();

    when(col("RiskCategory").eq(lit("Delta")))
    .then(
        when(col("RiskClass").eq(lit("CSR_nonSec")))
        .then(rf_rw_map(
            col("BucketCRR2"),
            csr_non_sec_weight_crr2,
            never_reached.clone(),
        ))
        .when(col("RiskClass").eq(lit("CSR_secCTP")))
        .then(rf_rw_map(
            col("BucketCRR2"),
            csr_sec_ctp_weight_crr2,
            never_reached,
        ))
        .otherwise(col("SensWeights")),
    )
    .when(col("RiskClass").eq(lit("DRC_NonSec")))
    .then(rf_rw_map(
        col("CreditQuality"),
        drc_nonsec_crr2,
        col("SensWeights"),
    ))
    .otherwise(col("SensWeights"))
}

fn bucket_weight_map(arr: &[f64]) -> HashMap<String, Expr> {
    let mut bucket_weights: HashMap<String, Expr> = HashMap::default();
    for (i, n) in arr.iter().enumerate() {
        let j = i + 1;
        bucket_weights.insert(
            format!["^{j}$"],
            Series::from_vec("weight", vec![*n; 1]).lit().list(),
        );
    }
    bucket_weights
}

// DRC Seniority
// as per 22.24
pub fn drc_seniority() -> Expr {
    when(col("RiskClass").eq(lit("DRC_NonSec")))
    .then(
        when(col("RiskFactorType").eq(lit("Covered")))
        .then(lit::<u8>(4))
        .when(col("RiskFactorType").eq(lit("SeniorSecured")))
        .then(lit::<u8>(3))
        .when(col("RiskFactorType").eq(lit("SeniorUnsecured")))
        .then(lit::<u8>(2))
        .when(col("RiskFactorType").eq(lit("Unrated")))
        .then(lit::<u8>(2))
        .when(col("RiskFactorType").eq(lit("NonSenior")))
        .then(lit::<u8>(1))
        .when(col("RiskFactorType").eq(lit("Equity")))
        .then(lit::<u8>(0))
        .otherwise(NULL.lit())
    )
    .otherwise(NULL.lit())
}




