//! Totals across different Risk Classes

use ultibi::polars::lazy::dsl::{col, max_exprs, Expr};
use ultibi::{BaseMeasure, DependantMeasure, Measure, PolarsResult, CPM};

use super::commodity::totals::*;
use super::csr_nonsec::totals::*;
use super::csr_sec_ctp::totals::*;
use super::csr_sec_nonctp::totals::*;
use super::equity::totals::*;
use super::fx::totals::*;
use super::girr::totals::*;

/// Expects three Exprs corresponding to Delta, Vega, Curvature
/// TODO check if that is fixed post 23.2
/// https://github.com/pola-rs/polars/issues/4659
///
/// *`expr` to contain at least one item
//pub(crate) fn total_sum(expr: &[Expr]) -> PolarsResult<Expr> {
//    apply_multiple(
//        move |columns| {
//            let mut res = unsafe { columns.get_unchecked(0) }.fill_null(FillNullStrategy::Zero);
//            for srs in columns.iter().skip(1) {
//                res = res?.add_to(&srs.fill_null(FillNullStrategy::Zero)?)
//            }
//            res
//        },
//        expr,
//        GetOutput::from_type(DataType::Float64),
//        false,
//    )
//}

fn sbm_charge_low(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_total_low(op)?
        + girr_total_low(op)?
        + eq_total_low(op)?
        + csrsecnonctp_total_low(op)?
        + com_total_low(op)?
        + csrnonsec_total_low(op)?
        + csrsecctp_total_low(op)?)
}
fn sbm_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_total_medium(op)?
        + girr_total_medium(op)?
        + eq_total_medium(op)?
        + csrsecnonctp_total_medium(op)?
        + com_total_medium(op)?
        + csrnonsec_total_medium(op)?
        + csrsecctp_total_medium(op)?)
}
fn sbm_charge_high(op: &CPM) -> PolarsResult<Expr> {
    Ok(fx_total_high(op)?
        + girr_total_high(op)?
        + eq_total_high(op)?
        + csrsecnonctp_total_high(op)?
        + com_total_high(op)?
        + csrnonsec_total_high(op)?
        + csrsecctp_total_high(op)?)
}

pub(crate) fn sbm_charge(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_exprs(&[
        sbm_charge_low(op)?,
        sbm_charge_medium(op)?,
        sbm_charge_high(op)?,
    ]))
}

// Testing Dependant Measures
fn sbm_charge_low_dep_test(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge Low")
        + col("GIRR TotalCharge Low")
        + col("EQ TotalCharge Low")
        + col("CSR Sec nonCTP TotalCharge Low")
        + col("CSR nonSec TotalCharge Low")
        + col("CSR secCTP TotalCharge Low")
        + col("Commodity TotalCharge Low"))
}

fn sbm_charge_medium_dep_test(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge Medium")
        + col("GIRR TotalCharge Medium")
        + col("EQ TotalCharge Medium")
        + col("CSR Sec nonCTP TotalCharge Medium")
        + col("CSR nonSec TotalCharge Medium")
        + col("CSR secCTP TotalCharge Medium")
        + col("Commodity TotalCharge Medium"))
}
fn sbm_charge_high_dep_test(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge High")
        + col("GIRR TotalCharge High")
        + col("EQ TotalCharge High")
        + col("CSR Sec nonCTP TotalCharge High")
        + col("CSR nonSec TotalCharge High")
        + col("CSR secCTP TotalCharge High")
        + col("Commodity TotalCharge High"))
}

pub(crate) fn sbm_charge_test(_: &CPM) -> PolarsResult<Expr> {
    Ok(max_exprs(&[
        col("SBM Charge High"),
        col("SBM Charge Medium"),
        col("SBM Charge Low"),
    ]))
}

pub(crate) fn sbm_total_measures() -> Vec<Measure> {
    let sbm_charge_low = Measure::Base(BaseMeasure {
        name: "SBM Charge Low Test".to_string(),
        calculator: Box::new(sbm_charge_low),
        aggregation: Some("scalar"),
        precomputefilter: None,
    });

    let sbm_charge_medium = Measure::Base(BaseMeasure {
        name: "SBM Charge Medium Test".to_string(),
        calculator: Box::new(sbm_charge_medium),
        aggregation: Some("scalar"),
        precomputefilter: None,
    });

    let sbm_charge_high = Measure::Base(BaseMeasure {
        name: "SBM Charge High Test".to_string(),
        calculator: Box::new(sbm_charge_high),
        aggregation: Some("scalar"),
        precomputefilter: None,
    });

    vec![
        sbm_charge_low,
        sbm_charge_medium,
        sbm_charge_high,
        Measure::Base(BaseMeasure {
            name: "SBM Charge Test".to_string(),
            calculator: Box::new(sbm_charge),
            aggregation: Some("scalar"),
            precomputefilter: None,
        }),
        // Testing dependency
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge Medium".to_string(),
            calculator: Box::new(sbm_charge_medium_dep_test),
            depends_upon: vec![
                ("FX TotalCharge Medium".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge Medium".to_string(), "scalar".to_string()),
                ("EQ TotalCharge Medium".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR secCTP TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge Low".to_string(),
            calculator: Box::new(sbm_charge_low_dep_test),
            depends_upon: vec![
                ("FX TotalCharge Low".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge Low".to_string(), "scalar".to_string()),
                ("EQ TotalCharge Low".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR secCTP TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge High".to_string(),
            calculator: Box::new(sbm_charge_high_dep_test),
            depends_upon: vec![
                ("FX TotalCharge High".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge High".to_string(), "scalar".to_string()),
                ("EQ TotalCharge High".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR secCTP TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge".to_string(),
            calculator: Box::new(sbm_charge_test),
            depends_upon: vec![
                ("SBM Charge Low".to_string(), "scalar".to_string()),
                ("SBM Charge Medium".to_string(), "scalar".to_string()),
                ("SBM Charge High".to_string(), "scalar".to_string()),
            ],
        }),
    ]
}
