//! This module includes complete weights allocation logic (pre build mode). SBM and DRC weights and Scale Factors
//!
//! TODO move all risk weights to a .csv/.json file and join/lookup. This would give flexibility with adjusting RiskWeights upon startup.

use crate::drc::drc_weights;
use once_cell::sync::OnceCell;
use polars::prelude::*;
use std::collections::HashMap;

/// 21.44 specified currencies
pub static REDUCED_IR_WEIGHT: &str = "EUR|USD|GBP|AUD|JPY|SEK|CAD";

static EQ_SPOT_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();
static EQ_REPO_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();

static COM_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_NONSEC_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_SECCTP_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_SECNONCTP_RW: OnceCell<LazyFrame> = OnceCell::new();

static VEGA_RW: OnceCell<LazyFrame> = OnceCell::new();
static VEGA_EQ_RW: OnceCell<LazyFrame> = OnceCell::new();

static DRC_NONSEC_RW: OnceCell<LazyFrame> = OnceCell::new();

/// This is just a helper to store parameters
pub struct SensWeightsConfig {
    /// Eq Vega, Com, CSR Delta
    rc_rcat_b_weights: LazyFrame,
    /// Eq Delta, GIRR Yield-Special
    rc_rcat_rtype_b_weights: LazyFrame,
    /// Girr XCCY, Infl, Yield-Base
    rc_rcat_rtype_weights: LazyFrame,
    /// All Vega
    rc_rcat_weights: LazyFrame,
    /// DRC Non Sec - Risk Cat, Risk Class, CreditQuality
    drc_nonsec_weights_frame: LazyFrame,
    drc_secnonctp_weights: LazyFrame,
    // FX
    fx: Expr,
    fx_override: HashMap<String, Expr>,
}

/// This function !Defines! Risk Weights as per regulation or where provided with build_params
/// Then calls [weight_assign_logic] where weights assignment actually happens
pub fn weights_assign(lf: LazyFrame, build_params: &HashMap<String, String>) -> PolarsResult<LazyFrame> {
    // FX  - can't be put into a frame due to regex requirement
    //21.88 - Conservative, false by default
    let fx_sqrt2_div = build_params
        .get("fx_sqrt2_div")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or_else(|| false);
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

    // GIRR  - can't be put into a frame due to regex requirement
    //21.44 - Conservative, false by default
    let girr_sqrt2_div = build_params
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
    let xccy_infl_weights = Series::new("Infl_XCCY", &[0.016]);

    let ir_base_srs = Series::new("", ir_base);

    let ir_base_srs_special = &ir_base_srs * girr_1_over_sqrt2;

    let girr_special_buckets: Vec<String> = REDUCED_IR_WEIGHT.split('|')
        .map(String::from)
        .collect();
    let girr_special_weights_per_bucket: Vec<Series> = girr_special_buckets.iter()
        .map(|_|ir_base_srs_special.clone())
        .collect();
    let girr_special_weights = rcat_rc_rft_b_weights_frame(&girr_special_weights_per_bucket, "Delta", "GIRR", "Yield", Some(girr_special_buckets))
        .lazy();
    let girr_rc_rcat_rtype_weights = girr_infl_xccy_weights_frame(xccy_infl_weights.clone(), xccy_infl_weights, ir_base_srs.clone());

    // Commodity
    // 21.82
    // Commodity risk is defined for 11 tenors
    let commodity_weights = [0.3, 0.35, 0.6, 0.8, 0.4, 0.45, 0.2, 0.35, 0.25, 0.35, 0.5];
    
    // check columns. Some of the cust weights files must contain these:
    let check_columns = [col("RiskClass").cast(DataType::Utf8), col("RiskCategory").cast(DataType::Utf8), col("BucketBCBS").cast(DataType::Utf8), col("Weights").cast(DataType::Float64)];
    let check_columns2 = [col("RiskClass").cast(DataType::Utf8), col("RiskCategory").cast(DataType::Utf8), col("BucketBCBS").cast(DataType::Utf8), col("Weights").cast(DataType::Float64), col("RiskFactorType").cast(DataType::Utf8),];
    let check_columns3 = [col("RiskClass").cast(DataType::Utf8), col("RiskCategory").cast(DataType::Utf8), col("Weights").cast(DataType::Float64)];
    let check_columns4 = [col("RiskClass").cast(DataType::Utf8), col("RiskCategory").cast(DataType::Utf8), col("CreditQuality").cast(DataType::Utf8), col("Weights").cast(DataType::Float64)];


    let _commodity_weights_frame = COM_DELTA_RW.get_or_init(|| {
            build_params.get("commodity_delta_weights")
            .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns).ok())
            .unwrap_or_else(||rcat_rc_b_weights_frame::<11>(&commodity_weights, "Delta", "Commodity"))
            .lazy() }) ;

    // Equity
    // 21.77
    let equity_spot_weights = [
        0.55, 0.6, 0.45, 0.55, 0.3, 0.35, 0.4, 0.5, 0.7, 0.5, 0.7, 0.15, 0.25,
    ].map(|x|Series::new("", [x]));

    let _equity_bucket_spot_weights = EQ_SPOT_DELTA_RW.get_or_init(|| {
            build_params.get("equity_spot_delta_weights")
            .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns2).ok())
            .unwrap_or_else(||rcat_rc_rft_b_weights_frame(&equity_spot_weights, "Delta", "Equity", "EqSpot", None))
            .lazy() }) ;

    let equity_repo_weights = [
        0.0055, 0.006, 0.0045, 0.0055, 0.003, 0.0035, 0.004, 0.005, 0.007, 0.005, 0.007, 0.0015,
        0.0025,
    ].map(|x|Series::new("", [x]));
    
    let _equity_bucket_repo_weights = EQ_REPO_DELTA_RW.get_or_init(|| {
            build_params.get("equity_repo_delta_weights")
            .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns2).ok())
            .unwrap_or_else(||rcat_rc_rft_b_weights_frame(&equity_repo_weights, "Delta", "Equity", "EqRepo", None))
            .lazy() }) ;

    // CSR nonsec
    // 21.53
    // same weight for all tenors, hence we simplify by keeping just one
    // TODO when arr gets fixed expand to all 5 tenors in order to use Total Delta/Total Weighted Delta
    let csr_nonsec_weights_arr = [
        0.005, 0.01, 0.05, 0.03, 0.03, 0.02, 0.015, 0.025, 0.02, 0.04, 0.12, 0.07, 0.085, 0.055,
        0.05, 0.12, 0.015, 0.05,
    ];

    let _csr_non_sec_weights = CSR_NONSEC_RW.get_or_init(|| {
        build_params.get("csr_non_sec_weights")
        .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns).ok())
        .unwrap_or_else(||rcat_rc_b_weights_frame::<5>(&csr_nonsec_weights_arr, "Delta", "CSR_nonSec"))
        .lazy() }) ;

    // CSR Sec CTP 21.59
    let csr_sec_ctp_weights_arr = [
        0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12, 0.12, 0.12,
        0.13,
    ];

    let _csr_sec_ctp_weights = CSR_SECCTP_RW.get_or_init(|| {
        build_params.get("csr_sec_ctp_weights")
        .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns).ok())
        .unwrap_or_else(||rcat_rc_b_weights_frame::<5>(&csr_sec_ctp_weights_arr, "Delta", "CSR_Sec_CTP"))
        .lazy() }) ;

    // CSR Sec nonCTP 21.62 and 325am
    let csr_sec_nonctp_weight_arr: [f64; 25] = [
        0.009, 0.015, 0.02, 0.02, 0.008, 0.012, 0.012, 0.014, 0.0125, 0.01875, 0.025, 0.025, 0.01,
        0.015, 0.015, 0.0175, 0.01575, 0.02625, 0.035, 0.035, 0.014, 0.021, 0.021, 0.0245, 0.035,
    ];

    let _csr_sec_nonctp_weigh = CSR_SECNONCTP_RW.get_or_init(|| {
            build_params.get("csr_sec_nonctp_weights")
            .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns).ok())
            .unwrap_or_else(||rcat_rc_b_weights_frame::<5>(&csr_sec_nonctp_weight_arr, "Delta", "CSR_Sec_nonCTP"))
            .lazy() }) ;

    let _vega_risk_class_weight = VEGA_RW.get_or_init(|| {
        build_params.get("vega_risk_weights")
        .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns3).ok())
        .unwrap_or_else(||vega_default_weights())
        .lazy() }) ;
    
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

    let _vega_equity_weight = VEGA_EQ_RW.get_or_init(|| {
        build_params.get("equity_vega_weights")
        .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns).ok())
        .unwrap_or_else(||rcat_rc_b_weights_frame::<5>(&equity_vega_weights, "Vega", "Equity"))
        .lazy() }) ;

    // Eq Vega, Com, CSR Delta
    let rc_rcat_b_weights = concat(&[
        _vega_equity_weight.clone(),
        _csr_sec_nonctp_weigh.clone(),
        _csr_sec_ctp_weights.clone(),
        _csr_non_sec_weights.clone(),
        _commodity_weights_frame.clone(),
    ], true, true)?; // we must never fail

    // Eq Delta
    let rc_rcat_rtype_b_weights = concat(&[
        _equity_bucket_spot_weights.clone(),
        _equity_bucket_repo_weights.clone(),
        girr_special_weights
    ], true, true)?;

    let rc_rcat_weights = concat(&[
        _vega_risk_class_weight.clone(),
        //TODO add GIRR
    ], true, true)?;

    let drc_nonsec_weights_frame = DRC_NONSEC_RW.get_or_init(|| {
        build_params.get("drc_nonsec_weights")
        .and_then(|some_string|frame_from_path_or_str(some_string, &check_columns4).ok())
        .unwrap_or_else(||drc_weights::dcr_nonsec_default_weights())
        .lazy() })
        .clone() ;

    let drc_secnonctp_weights: LazyFrame = drc_weights::_drc_secnonctp_weights_frame();

    let dlt_weights = SensWeightsConfig {
        rc_rcat_b_weights,
        rc_rcat_rtype_b_weights,
        rc_rcat_weights,
        rc_rcat_rtype_weights: girr_rc_rcat_rtype_weights,
        drc_nonsec_weights_frame,
        drc_secnonctp_weights,
        // FX
        fx: fx_base_srs.lit().list(),
        fx_override: fx_map,
    };

    //Assign Delta Weights
    weight_assign_logic(lf, dlt_weights)
}

pub fn weight_assign_logic(lf: LazyFrame, weights: SensWeightsConfig) -> PolarsResult<LazyFrame> {
    let x: Option<f64> = None;
    let not_yet_implemented = Series::new("null", &[x]).lit().list();

    // First, left join one by one
    let join_on = [col("RiskClass"), col("RiskCategory"), col("RiskFactorType"), col("BucketBCBS")];
    let mut lf1 = lf.join(weights.rc_rcat_rtype_b_weights, join_on.clone(), join_on, JoinType::Left);
    lf1 = lf1.rename(["Weights"], ["SensWeights"]);
    

    let join_on = [col("RiskClass"), col("RiskCategory"), col("BucketBCBS")];
    lf1 = lf1.join(weights.rc_rcat_b_weights, join_on.clone(), join_on, JoinType::Left);
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);
    

    let join_on = [col("RiskClass"), col("RiskCategory"), col("RiskFactorType")];
    lf1 = lf1.join(weights.rc_rcat_rtype_weights, join_on.clone(), join_on, JoinType::Left);
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);


    let join_on = [col("RiskClass"), col("RiskCategory")];
    let mut lf1 = lf1.join(weights.rc_rcat_weights, join_on.clone(), join_on, JoinType::Left);
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);
    
    let join_on = [col("RiskClass"), col("RiskCategory"), col("CreditQuality").map(
        |s| Ok(s.utf8()?.to_uppercase().into_series()),
        GetOutput::from_type(DataType::Utf8),
    )];
    let mut lf1 = lf1.join(weights.drc_nonsec_weights_frame, join_on.clone(), join_on, JoinType::Left);
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);
    
    //drc_secnonctp_weights
    let left_on = [col("RiskClass"), col("RiskCategory"), 
        concat_str(
        [
            col("CreditQuality").map(
                |s| Ok(s.utf8()?.to_uppercase().into_series()),
                GetOutput::from_type(DataType::Utf8),
            ),
            col("RiskFactorType").map(
                |s| Ok(s.utf8()?.to_uppercase().into_series()),
                GetOutput::from_type(DataType::Utf8),
            ),
        ],
        "_",
    ).alias("Key")];
    let right_on = [col("RiskClass"), col("RiskCategory"), col("Key")];
    let mut lf1 = lf1.join(weights.drc_secnonctp_weights, left_on, right_on, JoinType::Left);
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("RiskWeightDRC")))
        .select([col("*").exclude(["RiskWeightDRC", "Key"])]);

    // Delta
    lf1 = lf1.with_column(
    when(col("RiskCategory").eq(lit("Delta")))
        .then(
            // FX
            when(col("RiskClass").eq(lit("FX")))
            .then(rf_rw_map(
                col("BucketBCBS"),
                weights.fx_override,
                weights.fx,
                true
            ))
            .otherwise(not_yet_implemented.clone()),
    )
    .otherwise(not_yet_implemented)
    .alias("Weights")  
    );
    lf1 = lf1.with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);
    Ok(lf1)
}



/// returns Boolean series
/// uses .contains for regex
/// and .eq for strict equality
fn contains_or_equals(col: Expr, key: String, use_regeex: bool) -> Expr {
    if use_regeex {
        col.apply(
            move |s| Ok(s.utf8()?.contains(key.as_str())?.into_series()),
            GetOutput::from_type(DataType::Boolean),
        )
    } else {
        col.eq(lit(key))
    }
}

/// Calls .utf8() on `c` and iterates over map matching regex  
///
/// Potential optimisation to use &'static str instead of String (it has to be 'static due to apply)
/// *c - column to be compared
fn rf_rw_map(col: Expr, map: HashMap<String, Expr>, other: Expr, use_regex: bool) -> Expr {
    // buf is a placeholder
    let mut it = map.into_iter();
    let (k, v) = it.next().unwrap(); //The map will have at least one value

    let mut buf = when(lit::<bool>(false))
        .then(lit::<f64>(0.).list())
        .when(contains_or_equals(col.clone(), k, use_regex))
        .then(v);

    for (k, v) in it {
        buf = buf
            .when(contains_or_equals(col.clone(), k, use_regex))
            .then(v);
    }
    buf.otherwise(other)
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
    let csr_non_sec_weight_crr2: HashMap<String, Expr> = bucket_weight_map(&csr_nonsec_weights, 5);

    // CSR Sec CTP 21.59
    let csr_sec_ctp_weights_crr2 = [
        0.04, 0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.03, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12,
        0.12, 0.12, 0.13,
    ];
    let csr_sec_ctp_weight_crr2: HashMap<String, Expr> =
        bucket_weight_map(&csr_sec_ctp_weights_crr2, 5);

    let drc_nonsec_crr2 = drc_weights::drc_nonsec_weights_crr2();

    when(col("RiskCategory").eq(lit("Delta")))
        .then(
            when(col("RiskClass").eq(lit("CSR_nonSec")))
                .then(rf_rw_map(
                    col("BucketCRR2"),
                    csr_non_sec_weight_crr2,
                    never_reached.clone(),
                    false,
                ))
                .when(col("RiskClass").eq(lit("CSR_secCTP")))
                .then(rf_rw_map(
                    col("BucketCRR2"),
                    csr_sec_ctp_weight_crr2,
                    never_reached,
                    false
                ))
                .otherwise(col("SensWeights")),
        )
        .when(col("RiskClass").eq(lit("DRC_nonSec")))
        .then(rf_rw_map(
            col("CreditQuality"),
            drc_nonsec_crr2,
            col("SensWeights"),
            true
        ))
        .otherwise(col("SensWeights"))
}

fn bucket_weight_map(arr: &[f64], ntenors: u8) -> HashMap<String, Expr> {
    let mut bucket_weights: HashMap<String, Expr> = HashMap::default();
    for (i, n) in arr.iter().enumerate() {
        let j = i + 1;
        bucket_weights.insert(
            format!["{j}"],
            Series::from_vec("weight", vec![*n; ntenors as usize])
                .lit()
                .list(),
        );
    }
    bucket_weights
}

/// Frame: Risk Category, Risk Class, Bucket, Risk Weight
pub fn rcat_rc_b_weights_frame<const NTENORS: usize>(weights: &[f64], rcat: &str, rc: &str) -> DataFrame {
    let weights_list: [Expr; NTENORS] = ["Weights"; NTENORS]
        .into_iter()
        .map(|w|col(w))
        .collect::<Vec<Expr>>()
        .try_into()
        .expect("Couldn't produce weights list");

    df!(
        "RiskClass" => vec![rc; weights.len()],
        "RiskCategory" => vec![rcat; weights.len()],
        "BucketBCBS" => weights.iter().enumerate().map(|(i, _)| (i+1).to_string()).collect::<Vec<String>>(),
        "Weights" => weights,
    )
    .and_then(|df|
         df.lazy()
        .with_column(concat_lst(weights_list).alias("Weights"))
        .collect()
    )
    .expect("Couldn't build default frame") // we should never fail on default frames!
}
/// Frame: Risk Category, Risk Class, RFType, Bucket, Risk Weight
/// NOTE: can't use the same single funtion with rtf: Option<&str> because
/// CSR RFtypes are Bond and CDS, won't match to None
pub fn rcat_rc_rft_b_weights_frame(weights: &[Series], rcat: &str, rc: &str, rft: &str, buckets: Option<Vec<String>>) -> DataFrame {
    
    let _buckets = buckets
        .unwrap_or_else(||{
            weights.iter().enumerate().map(|(i, _)| (i+1).to_string()).collect::<Vec<String>>()
        });

    df!(
        "RiskClass" => vec![rc; weights.len()],
        "RiskCategory" => vec![rcat; weights.len()],
        "RiskFactorType" => vec![rft; weights.len()],
        "BucketBCBS" => _buckets,
        "Weights" => weights,
    )
    .expect("Couldn't build default frame") // we should never fail on default frames!
}

fn vega_default_weights() -> DataFrame {
    let s0 = Series::new("GIRR", &[vega_rw(60); 5]);
    let s1 = Series::new("CSR_nonSec", &[vega_rw(120); 5]);
    let s2 = Series::new("CSR_Sec_CTP", &[vega_rw(120); 5]);
    let s3 = Series::new("CSR_Sec_nonCTP", &[vega_rw(120); 5]);
    let s4 = Series::new("Commodity", &[vega_rw(120); 5]);
    let s5 = Series::new("FX", &[vega_rw(40); 5]);

    let list = Series::new("Weights", &[s0, s1, s2, s3, s4, s5]);
    let s6 = Series::new("RiskClass", &["GIRR", "CSR_nonSec", "CSR_Sec_CTP", "CSR_Sec_nonCTP", "Commodity", "FX"]);
    let s7 = Series::new("RiskCategory", &["Vega"; 6]);

    DataFrame::new(vec![list, s6, s7])
    .expect("Couldn't get Vega Weights Frame")    
}

/// 21.43
/// Girr Risk weight
pub(crate) fn girr_infl_xccy_weights_frame(xccy_weights: Series, infl_weights: Series, yield_weights: Series) -> LazyFrame {
    df![
        "Weights" => vec![xccy_weights, infl_weights, yield_weights],
        "RiskFactorType" => ["XCCY", "Inflation", "Yield"],
        "RiskClass" => vec!["GIRR"; 3],
        "RiskCategory" => vec!["Delta"; 3],
    ]
    .unwrap() // We must not fail on default frame
    .lazy()
    .with_column(concat_lst([col("Weights")]))
}

/// Checks if `some_str` is a path
/// If not tries to serialise it
/// Checks for expected columns
pub fn frame_from_path_or_str(some_str: &str, check_columns: &[Expr]) -> PolarsResult<DataFrame> {
    CsvReader::from_path(some_str)
    .and_then(|csv|csv.has_header(true).finish())
    .and_then(|df|df.lazy().select(check_columns).collect())
    .or_else(|_|{
        serde_json::from_str::<DataFrame>(some_str)
        .map_err(|_|PolarsError::InvalidOperation("couldn't convert string to frame".into()))
        .and_then(|df| df.lazy().select(check_columns).collect())
    }) 
}

fn vega_rw(lh: u8) -> f64 {
    (0.55 * (lh as f64).sqrt() / (10f64).sqrt()).min(1.)
}