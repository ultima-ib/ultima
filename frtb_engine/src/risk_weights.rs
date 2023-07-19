//! This module includes complete weights allocation logic (pre build mode). SBM and DRC weights and Scale Factors
//! It follows the logic prescribed by the text https://www.bis.org/bcbs/publ/d457.pdf
//! TODO Weights column is a list. We can't read it as list from a csv. frame_from_path_or_str must concat_lst columns which have Sens in the name

use crate::drc::drc_weights;
use once_cell::sync::OnceCell;
use std::collections::BTreeMap;
use ultibi::polars::prelude::diag_concat_lf;
use ultibi::polars::prelude::{
    col, concat_list, concat_str, df, lit, CsvReader, DataFrame, DataType, Expr, GetOutput,
    IntoLazy, IntoSeries, JoinType, LazyFrame, NamedFrom, PolarsError, PolarsResult, SerReader,
    Series, Utf8NameSpaceImpl,
};

static FX_SPECIAL_DELTA_FULL_RW: OnceCell<LazyFrame> = OnceCell::new();
static FX_SPECIAL_DELTA_PARTIAL_RW: OnceCell<LazyFrame> = OnceCell::new();
static FX_BASE_DELRA_RW: OnceCell<LazyFrame> = OnceCell::new();

/// 21.44 specified currencies
pub static REDUCED_IR_WEIGHT: &str = "EUR|USD|GBP|AUD|JPY|SEK|CAD";
static GIRR_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();
static GIRR_DELTA_SPECIAL_RW: OnceCell<LazyFrame> = OnceCell::new();

static EQ_SPOT_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();
static EQ_REPO_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();

static COM_DELTA_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_NONSEC_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_SECCTP_RW: OnceCell<LazyFrame> = OnceCell::new();
static CSR_SECNONCTP_RW: OnceCell<LazyFrame> = OnceCell::new();

static VEGA_RW: OnceCell<LazyFrame> = OnceCell::new();
static VEGA_EQ_RW: OnceCell<LazyFrame> = OnceCell::new();

static DRC_NONSEC_RW: OnceCell<LazyFrame> = OnceCell::new();
static DRC_SECNONCTP_RW: OnceCell<LazyFrame> = OnceCell::new();

/// This is just a helper to store parameters
pub struct SensWeightsConfig {
    /// Eq Vega, Com, CSR Delta, FX Special cases
    rc_rcat_b_weights: LazyFrame,
    /// FX Special cases where only first three chars used
    rc_rcat_b_weights_second: LazyFrame,
    /// Eq Delta, GIRR Yield-Special
    rc_rcat_rtype_b_weights: LazyFrame,
    /// Girr XCCY, Infl, Yield-Base
    rc_rcat_rtype_weights: LazyFrame,
    /// All Vega
    rc_rcat_weights: LazyFrame,
    /// DRC Non Sec - Risk Cat, Risk Class, CreditQuality
    drc_nonsec_weights_frame: LazyFrame,
    drc_secnonctp_weights: LazyFrame,
}

/// This function !Defines! Risk Weights as per regulation or where provided with build_params
/// Then calls [weight_assign_logic] where weights assignment actually happens
pub fn weights_assign(
    lf: LazyFrame,
    build_params: &BTreeMap<String, String>,
) -> PolarsResult<LazyFrame> {
    // check columns. Some of the cast weights files must contain these:
    let check_columns0 = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("RiskFactorType").cast(DataType::Utf8),
        col("Weights").cast(DataType::Utf8),
    ];
    let check_columns_rc_rcat_b_w = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("BucketBCBS").cast(DataType::Utf8),
        col("Weights").cast(DataType::Utf8),
    ];
    let check_columns_rcat_rc_rft_b_w = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("BucketBCBS").cast(DataType::Utf8),
        col("Weights").cast(DataType::Utf8),
        col("RiskFactorType").cast(DataType::Utf8),
    ];
    let check_columns_rc_rcat_w = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("Weights").cast(DataType::Utf8),
    ];
    let check_columns4 = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("CreditQuality").cast(DataType::Utf8),
        col("Weights").cast(DataType::Utf8),
    ];
    let check_columns_key_rc_rcat_drcrw = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("Key").cast(DataType::Utf8),
        col("RiskWeightDRC").cast(DataType::Utf8),
    ];

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

    // Note ...EUR are CRR2 specific risk weights
    let fx_buckets_first = vec![
        "HRKEUR".to_string(),
        "BGNEUR".to_string(),
        "DKKEUR".to_string(),
        "USDUSD".to_string(),
        "EUREUR".to_string(),
    ];
    let fx_weights_first = vec![
        Series::new("", &[0.05]),
        Series::new("", &[0.05]),
        Series::new("", &[0.0225]),
        Series::new("", &[0.]),
        Series::new("", &[0.]),
    ];

    let fx_rc_rcat_b_first = FX_SPECIAL_DELTA_FULL_RW.get_or_init(|| {
        build_params
            .get("fx_special_delta_weights_full")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &fx_weights_first,
                    "Delta",
                    "FX",
                    None,
                    None,
                    Some(fx_buckets_first),
                )
            })
            .lazy()
    });

    // These FX Buckets to also be joined on Bucket, but using only first three chars
    // as per 21.88
    let fx_buckets_second = vec![
        "USD".to_string(),
        "EUR".to_string(),
        "JPY".to_string(),
        "GBP".to_string(),
        "AUD".to_string(),
        "CAD".to_string(),
        "CHF".to_string(),
        "MXN".to_string(),
        "CNY".to_string(),
        "CNO".to_string(),
        "NZD".to_string(),
        "RUB".to_string(),
        "HKD".to_string(),
        "SGD".to_string(),
        "TRY".to_string(),
        "KRW".to_string(),
        "SEK".to_string(),
        "ZAR".to_string(),
        "NOK".to_string(),
        "BRL".to_string(),
        "KRW".to_string(),
        "DKK".to_string(),
    ];
    let fx_weights_second = fx_buckets_second
        .iter()
        .map(|_| Series::new("", fx_base) * fx_1_over_sqrt2)
        .collect::<Vec<Series>>();

    let fx_rc_rcat_b_second = FX_SPECIAL_DELTA_PARTIAL_RW.get_or_init(|| {
        build_params
            .get("fx_special_delta_weights_partial")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &fx_weights_second,
                    "Delta",
                    "FX",
                    None,
                    Some("Bucket"),
                    Some(fx_buckets_second),
                )
            })
            .lazy()
    });

    let fx_base_weights = FX_BASE_DELRA_RW.get_or_init(|| {
        build_params
            .get("fx_delta_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                df!(
                    "Weights" => vec![fx_base_srs],
                    "RiskClass" => ["FX"],
                    "RiskCategory" => ["Delta"]
                )
                .expect("Couldn't build base FX weights") // we must never fail on default frame
            })
            .lazy()
    });

    // GIRR  - can't be put into a frame due to regex requirement
    // 21.44 - Conservative, false by default
    let girr_sqrt2_div = build_params
        .get("girr_sqrt2_div")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let girr_1_over_sqrt2 = if girr_sqrt2_div {
        1. / 2.0_f64.sqrt()
    } else {
        1_f64
    };
    // 21.42
    let ir_base = &[
        0., 0.017, 0.017, 0.016, 0.013, 0.012, 0.011, 0.011, 0.011, 0.011, 0.011,
    ];
    // 21.43
    let xccy_infl_weights = Series::new("Infl_XCCY", &[0.016]);

    let ir_base_srs = Series::new("", ir_base);

    let ir_base_srs_special = &ir_base_srs * girr_1_over_sqrt2;

    let girr_special_buckets: Vec<String> =
        REDUCED_IR_WEIGHT.split('|').map(String::from).collect();
    let girr_special_weights_per_bucket: Vec<Series> = girr_special_buckets
        .iter()
        .map(|_| ir_base_srs_special.clone())
        .collect();

    let girr_special_weights = GIRR_DELTA_SPECIAL_RW.get_or_init(|| {
        build_params
            .get("girr_delta_special_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rcat_rc_rft_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_rft_b_weights_frame(
                    &girr_special_weights_per_bucket,
                    "Delta",
                    "GIRR",
                    "Yield",
                    Some(girr_special_buckets),
                )
            })
            .lazy()
    });

    let girr_rc_rcat_rtype_weights = GIRR_DELTA_RW.get_or_init(|| {
        build_params
            .get("girr_delta_base_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns0, "Weights").ok()
            })
            .unwrap_or_else(|| {
                girr_infl_xccy_weights_frame(
                    xccy_infl_weights.clone(),
                    xccy_infl_weights,
                    ir_base_srs,
                )
            })
            .lazy()
    });

    // Commodity
    // 21.82
    // Commodity risk is defined for 11 tenors
    let commodity_weights = [0.3, 0.35, 0.6, 0.8, 0.4, 0.45, 0.2, 0.35, 0.25, 0.35, 0.5]
        .map(|x| Series::new("", [x; 11]));

    let _commodity_weights_frame = COM_DELTA_RW.get_or_init(|| {
        build_params
            .get("commodity_delta_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(&commodity_weights, "Delta", "Commodity", None, None, None)
            })
            .lazy()
    });

    // Equity
    // 21.77
    let equity_spot_weights = [
        0.55, 0.6, 0.45, 0.55, 0.3, 0.35, 0.4, 0.5, 0.7, 0.5, 0.7, 0.15, 0.25,
    ]
    .map(|x| Series::new("", [x]));

    let _equity_bucket_spot_weights = EQ_SPOT_DELTA_RW.get_or_init(|| {
        build_params
            .get("equity_spot_delta_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rcat_rc_rft_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_rft_b_weights_frame(&equity_spot_weights, "Delta", "Equity", "EqSpot", None)
            })
            .lazy()
    });

    let equity_repo_weights = [
        0.0055, 0.006, 0.0045, 0.0055, 0.003, 0.0035, 0.004, 0.005, 0.007, 0.005, 0.007, 0.0015,
        0.0025,
    ]
    .map(|x| Series::new("", [x]));

    let _equity_bucket_repo_weights = EQ_REPO_DELTA_RW.get_or_init(|| {
        build_params
            .get("equity_repo_delta_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rcat_rc_rft_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_rft_b_weights_frame(&equity_repo_weights, "Delta", "Equity", "EqRepo", None)
            })
            .lazy()
    });

    // CSR nonsec
    // 21.53
    // same weight for all tenors, hence we simplify by keeping just one
    // TODO when arr gets fixed expand to all 5 tenors in order to use Total Delta/Total Weighted Delta
    let csr_nonsec_weights_arr = [
        0.005, 0.01, 0.05, 0.03, 0.03, 0.02, 0.015, 0.025, 0.02, 0.04, 0.12, 0.07, 0.085, 0.055,
        0.05, 0.12, 0.015, 0.05,
    ]
    .map(|x| Series::new("", [x; 5]));

    let _csr_non_sec_weights = CSR_NONSEC_RW.get_or_init(|| {
        build_params
            .get("csr_non_sec_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &csr_nonsec_weights_arr,
                    "Delta",
                    "CSR_nonSec",
                    None,
                    None,
                    None,
                )
            })
            .lazy()
    });

    // CSR Sec CTP 21.59
    let csr_sec_ctp_weights_arr = [
        0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12, 0.12, 0.12,
        0.13,
    ]
    .map(|x| Series::new("", [x; 5]));

    let _csr_sec_ctp_weights = CSR_SECCTP_RW.get_or_init(|| {
        build_params
            .get("csr_sec_ctp_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &csr_sec_ctp_weights_arr,
                    "Delta",
                    "CSR_Sec_CTP",
                    None,
                    None,
                    None,
                )
            })
            .lazy()
    });

    // CSR Sec nonCTP 21.62 and 325am
    let csr_sec_nonctp_weight_arr = [
        0.009, 0.015, 0.02, 0.02, 0.008, 0.012, 0.012, 0.014, 0.0125, 0.01875, 0.025, 0.025, 0.01,
        0.015, 0.015, 0.0175, 0.01575, 0.02625, 0.035, 0.035, 0.014, 0.021, 0.021, 0.0245, 0.035,
    ]
    .map(|x| Series::new("", [x; 5]));

    let _csr_sec_nonctp_weigh = CSR_SECNONCTP_RW.get_or_init(|| {
        build_params
            .get("csr_sec_nonctp_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &csr_sec_nonctp_weight_arr,
                    "Delta",
                    "CSR_Sec_nonCTP",
                    None,
                    None,
                    None,
                )
            })
            .lazy()
    });

    let _vega_risk_class_weight = VEGA_RW.get_or_init(|| {
        build_params
            .get("vega_risk_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_w, "Weights").ok()
            })
            .unwrap_or_else(vega_default_weights)
            .lazy()
    });
    //dbg!(_vega_risk_class_weight.clone().collect());

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
    ]
    .map(|x| Series::new("", [x; 5]));

    let _vega_equity_weight = VEGA_EQ_RW.get_or_init(|| {
        build_params
            .get("equity_vega_weights")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns_rc_rcat_b_w, "Weights").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(&equity_vega_weights, "Vega", "Equity", None, None, None)
            })
            .lazy()
    });

    // Eq Vega, Com, CSR Delta
    let rc_rcat_b_weights = diag_concat_lf(
        &[
            _vega_equity_weight.clone(),
            _csr_sec_nonctp_weigh.clone(),
            _csr_sec_ctp_weights.clone(),
            _csr_non_sec_weights.clone(),
            _commodity_weights_frame.clone(),
            fx_rc_rcat_b_first.clone(),
        ],
        true,
        true,
    )?; // we must never fail

    // Eq Delta
    let rc_rcat_rtype_b_weights = diag_concat_lf(
        &[
            _equity_bucket_spot_weights.clone(),
            _equity_bucket_repo_weights.clone(),
            girr_special_weights.clone(),
        ],
        true,
        true,
    )?;

    let rc_rcat_weights = diag_concat_lf(
        &[_vega_risk_class_weight.clone(), fx_base_weights.clone()],
        true,
        true,
    )?;

    let drc_nonsec_weights_frame = DRC_NONSEC_RW
        .get_or_init(|| {
            build_params
                .get("drc_nonsec_weights")
                .and_then(|some_string| {
                    frame_from_path_or_str(some_string, &check_columns4, "Weights").ok()
                })
                .unwrap_or_else(drc_weights::dcr_nonsec_default_weights)
                .lazy()
        })
        .clone();

    let drc_secnonctp_weights: LazyFrame = DRC_SECNONCTP_RW
        .get_or_init(|| {
            build_params
                .get("drc_secnonctp_weights")
                .and_then(|some_string| {
                    frame_from_path_or_str(some_string, &check_columns_key_rc_rcat_drcrw, "Weights")
                        .ok()
                })
                .unwrap_or_else(drc_weights::_drc_secnonctp_weights_frame)
                .lazy()
        })
        .clone();

    let dlt_weights = SensWeightsConfig {
        rc_rcat_b_weights,
        rc_rcat_b_weights_second: fx_rc_rcat_b_second.clone(),
        rc_rcat_rtype_b_weights,
        rc_rcat_weights,
        rc_rcat_rtype_weights: girr_rc_rcat_rtype_weights.clone(),
        drc_nonsec_weights_frame,
        drc_secnonctp_weights,
    };

    //Assign Delta Weights
    weight_assign_logic(lf, dlt_weights)
}

/// This is where wheights assignments actually happens
pub fn weight_assign_logic(lf: LazyFrame, weights: SensWeightsConfig) -> PolarsResult<LazyFrame> {
    // First, left join one by one
    let join_on = [
        col("RiskClass"),
        col("RiskCategory"),
        col("RiskFactorType"),
        col("BucketBCBS"),
    ];
    let mut lf1 = lf.join(
        weights.rc_rcat_rtype_b_weights,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    // tmp workaround, since .rename() panics
    // TODO remove
    // Adding here to check if weights were assigned, if not - create a dummy column
    if !lf1.schema()?.iter_names().any(|col| col == "Weights") {
        lf1 = lf1.with_column(lit(0.).implode().alias("SensWeights"))
    } else {
        lf1 = lf1.rename(["Weights"], ["SensWeights"])
    }

    //dbg!(lf1.clone().filter(col("RiskClass").eq(lit("FX"))).select([col("BucketBCBS"), col("RiskFactor"), col("SensWeights")]).collect());

    let join_on = [col("RiskClass"), col("RiskCategory"), col("BucketBCBS")];
    lf1 = lf1.join(
        weights.rc_rcat_b_weights,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);

    // FX SECOND HERE
    lf1 = lf1.with_column(
        col("BucketBCBS")
            .map(
                |s| Ok(Some(s.utf8()?.str_slice(0, Some(3))?.into_series())),
                GetOutput::from_type(DataType::Utf8),
            )
            .alias("Bucket"),
    );
    let right_on = [col("RiskClass"), col("RiskCategory"), col("Bucket")];
    lf1 = lf1.join(
        weights.rc_rcat_b_weights_second,
        right_on.clone(),
        right_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights", "Bucket"])]);

    let join_on = [col("RiskClass"), col("RiskCategory"), col("RiskFactorType")];
    lf1 = lf1.join(
        weights.rc_rcat_rtype_weights,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);

    let join_on = [col("RiskClass"), col("RiskCategory")];
    let mut lf1 = lf1.join(
        weights.rc_rcat_weights,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);

    let join_on = [
        col("RiskClass"),
        col("RiskCategory"),
        col("CreditQuality").map(
            |s| Ok(Some(s.utf8()?.to_uppercase().into_series())),
            GetOutput::from_type(DataType::Utf8),
        ),
    ];
    let mut lf1 = lf1.join(
        weights.drc_nonsec_weights_frame,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("Weights")))
        .select([col("*").exclude(["Weights"])]);

    //drc_secnonctp_weights
    lf1 = lf1.with_column(
        concat_str(
            [
                col("CreditQuality").map(
                    |s| Ok(Some(s.utf8()?.to_uppercase().into_series())),
                    GetOutput::from_type(DataType::Utf8),
                ),
                col("RiskFactorType").map(
                    |s| Ok(Some(s.utf8()?.to_uppercase().into_series())),
                    GetOutput::from_type(DataType::Utf8),
                ),
            ],
            "_",
        )
        .alias("Key"),
    );

    let left_on = [col("RiskClass"), col("RiskCategory"), col("Key")];
    let right_on = [col("RiskClass"), col("RiskCategory"), col("Key")];
    let mut lf1 = lf1.join(
        weights.drc_secnonctp_weights,
        left_on,
        right_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeights").fill_null(col("RiskWeightDRC")))
        .select([col("*").exclude(["RiskWeightDRC", "Key"])]);

    Ok(lf1)
}

/// Frame: Risk Category, Risk Class, Bucket, Risk Weight
pub fn rcat_rc_b_weights_frame(
    weights: &[Series],
    rcat: &str,
    rc: &str,
    weights_col_name: Option<&str>,
    bucket_col_name: Option<&str>,
    buckets: Option<Vec<String>>,
) -> DataFrame {
    let weights_col_name = weights_col_name.unwrap_or_else(|| "Weights");
    let bucket_col_name = bucket_col_name.unwrap_or_else(|| "BucketBCBS");
    let _buckets = buckets.unwrap_or_else(|| {
        weights
            .iter()
            .enumerate()
            .map(|(i, _)| (i + 1).to_string())
            .collect::<Vec<String>>()
    });

    df!(
        "RiskClass" => vec![rc; weights.len()],
        "RiskCategory" => vec![rcat; weights.len()],
        bucket_col_name => _buckets,
        weights_col_name => weights,
    )
    .expect("Couldn't build default frame") // we should never fail on default frames!
}
/// Frame: Risk Category, Risk Class, RFType, Bucket, Risk Weight
/// NOTE: can't use the same single funtion with rtf: Option<&str> because
/// CSR RFtypes are Bond and CDS, won't match to None
pub fn rcat_rc_rft_b_weights_frame(
    weights: &[Series],
    rcat: &str,
    rc: &str,
    rft: &str,
    buckets: Option<Vec<String>>,
) -> DataFrame {
    let _buckets = buckets.unwrap_or_else(|| {
        weights
            .iter()
            .enumerate()
            .map(|(i, _)| (i + 1).to_string())
            .collect::<Vec<String>>()
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
    let s6 = Series::new(
        "RiskClass",
        &[
            "GIRR",
            "CSR_nonSec",
            "CSR_Sec_CTP",
            "CSR_Sec_nonCTP",
            "Commodity",
            "FX",
        ],
    );
    let s7 = Series::new("RiskCategory", &["Vega"; 6]);

    DataFrame::new(vec![list, s6, s7]).expect("Couldn't get Vega Weights Frame")
}

/// 21.43
/// Girr Risk weight
pub(crate) fn girr_infl_xccy_weights_frame(
    xccy_weights: Series,
    infl_weights: Series,
    yield_weights: Series,
) -> DataFrame {
    df![
        "Weights" => vec![xccy_weights, infl_weights, yield_weights],
        "RiskFactorType" => ["XCCY", "Inflation", "Yield"],
        "RiskClass" => vec!["GIRR"; 3],
        "RiskCategory" => vec!["Delta"; 3],
    ]
    .unwrap() // We must not fail on default frame
    .lazy()
    .with_column(concat_list([col("Weights")]).unwrap()) // don't expect to fail on default
    .collect()
    .expect("failed on IR Delta weights") // We must not fail on default frame
}

/// Checks if `some_str` is a path
/// If not tries to serialise it
/// Checks for expected columns
pub fn frame_from_path_or_str(
    some_str: &str,
    check_columns: &[Expr],
    cast_to_lst_f64: &str,
) -> PolarsResult<DataFrame> {
    let df = if let Ok(csv) = CsvReader::from_path(some_str) {
        csv.has_header(true)
            .finish()?
            .lazy()
            .select(check_columns)
            .collect()
    } else {
        serde_json::from_str::<DataFrame>(some_str)
            .map_err(|_| PolarsError::InvalidOperation("couldn't serialise string to frame".into()))
            .and_then(|df| df.lazy().select(check_columns).collect())
    }?;

    df_split_weights(df, cast_to_lst_f64)
}

fn df_split_weights(df: DataFrame, name: &str) -> PolarsResult<DataFrame> {
    df.lazy()
        .with_column(
            col(name)
                .str()
                .split(";")
                .cast(DataType::List(Box::new(DataType::Float64))),
        )
        .collect()
}

fn vega_rw(lh: u8) -> f64 {
    (0.55 * (lh as f64).sqrt() / (10f64).sqrt()).min(1.)
}
