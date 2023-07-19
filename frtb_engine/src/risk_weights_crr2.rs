//! This module is for CRR2 specific Risk weights
//! https://www.eba.europa.eu/regulation-and-policy/single-rulebook/interactive-single-rulebook/108255 325ae onward
use std::collections::BTreeMap;

use once_cell::sync::OnceCell;
use ultibi::polars::prelude::diag_concat_lf;
use ultibi::polars::prelude::{
    col, DataType, GetOutput, IntoLazy, IntoSeries, JoinType, LazyFrame, NamedFrom, PolarsResult,
    Series, Utf8NameSpaceImpl,
};

use crate::{
    drc::drc_weights,
    prelude::{frame_from_path_or_str, rcat_rc_b_weights_frame},
};

static CSR_NONSEC_RW_CRR2: OnceCell<LazyFrame> = OnceCell::new();
static CSR_SECCTP_RW_CRR2: OnceCell<LazyFrame> = OnceCell::new();
static DRC_NONSEC_RW_CRR2: OnceCell<LazyFrame> = OnceCell::new();

/// This is just a helper to store parameters
/// CRR2 - this a cases where CRR2 weight IS DIFFERENT to BCBS weight

pub struct SensWeightsConfig {
    rc_rcat_b_weights_crr2: LazyFrame,
    drc_nonsec_weights_frame_crr2: LazyFrame,
}

/// That's all for BCBS param set. Now, add CRR2
/// Note: CRR2 weights have to be joined on BucketCRR2, not BucketBCBS
pub fn weights_assign_crr2(
    lf: LazyFrame,
    build_params: &BTreeMap<String, String>,
) -> PolarsResult<LazyFrame> {
    let check_columns = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("BucketCRR2").cast(DataType::Utf8),
        col("WeightsCRR2").cast(DataType::Utf8),
    ];
    let check_columns4 = [
        col("RiskClass").cast(DataType::Utf8),
        col("RiskCategory").cast(DataType::Utf8),
        col("CreditQuality").cast(DataType::Utf8),
        col("WeightsCRR2").cast(DataType::Utf8),
    ];

    let csr_nonsec_weights_crr2 = [
        0.005, 0.005, 0.01, 0.05, 0.03, 0.03, 0.02, 0.015, 0.1, 0.025, 0.02, 0.04, 0.12, 0.07,
        0.085, 0.055, 0.05, 0.12, 0.015, 0.05,
    ]
    .map(|x| Series::new("", [x; 5]));
    let csr_non_sec_weights_frane_crr2 = CSR_NONSEC_RW_CRR2.get_or_init(|| {
        build_params
            .get("csr_non_sec_weights_crr2")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns, "WeightsCRR2").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &csr_nonsec_weights_crr2,
                    "Delta",
                    "CSR_nonSec",
                    Some("WeightsCRR2"),
                    Some("BucketCRR2"),
                    None,
                )
            })
            .lazy()
    });

    let csr_sec_ctp_weights_crr2 = [
        0.04, 0.04, 0.04, 0.08, 0.05, 0.04, 0.03, 0.02, 0.03, 0.06, 0.13, 0.13, 0.16, 0.10, 0.12,
        0.12, 0.12, 0.13,
    ]
    .map(|x| Series::new("", [x; 5]));

    let csr_sec_ctp_weights_frame_crr2 = CSR_SECCTP_RW_CRR2.get_or_init(|| {
        build_params
            .get("csr_non_sec_weights_crr2")
            .and_then(|some_string| {
                frame_from_path_or_str(some_string, &check_columns, "WeightsCRR2").ok()
            })
            .unwrap_or_else(|| {
                rcat_rc_b_weights_frame(
                    &csr_sec_ctp_weights_crr2,
                    "Delta",
                    "CSR_Sec_CTP",
                    Some("WeightsCRR2"),
                    Some("BucketCRR2"),
                    None,
                )
            })
            .lazy()
    });
    let rc_rcat_b_weights_crr2 = diag_concat_lf(
        &[
            csr_non_sec_weights_frane_crr2.clone(),
            csr_sec_ctp_weights_frame_crr2.clone(),
        ],
        true,
        true,
    )?; // we must never fail

    //rc_rcat_b_weights = rc_rcat_b_weights.join(rc_rcat_b_weights_crr2, );

    let drc_nonsec_weights_frame_crr2 = DRC_NONSEC_RW_CRR2
        .get_or_init(|| {
            build_params
                .get("drc_nonsec_weights_crr2")
                .and_then(|some_string| {
                    frame_from_path_or_str(some_string, &check_columns4, "WeightsCRR2").ok()
                })
                .unwrap_or_else(drc_weights::drc_nonsec_weights_frame_crr2)
                .lazy()
        })
        .clone();

    let weights = SensWeightsConfig {
        rc_rcat_b_weights_crr2,
        drc_nonsec_weights_frame_crr2,
    };

    weight_assign_logic_crr2(lf, weights)
}

pub fn weight_assign_logic_crr2(
    lf: LazyFrame,
    weights: SensWeightsConfig,
) -> PolarsResult<LazyFrame> {
    // First, left join one by one
    let join_on = [col("RiskClass"), col("RiskCategory"), col("BucketCRR2")];
    let mut lf1 = lf.join(
        weights.rc_rcat_b_weights_crr2,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1.rename(["WeightsCRR2"], ["SensWeightsCRR2"]);

    let join_on = [
        col("RiskClass"),
        col("RiskCategory"),
        col("CreditQuality").map(
            |s| Ok(Some(s.utf8()?.to_uppercase().into_series())),
            GetOutput::from_type(DataType::Utf8),
        ),
    ];
    let mut lf1 = lf1.join(
        weights.drc_nonsec_weights_frame_crr2,
        join_on.clone(),
        join_on,
        JoinType::Left.into(),
    );
    lf1 = lf1
        .with_column(col("SensWeightsCRR2").fill_null(col("WeightsCRR2")))
        .select([col("*").exclude(["WeightsCRR2"])]);

    // Finally, fill with default weights of BCBS
    lf1 = lf1.with_column(col("SensWeightsCRR2").fill_null(col("SensWeights")));

    Ok(lf1)
}
