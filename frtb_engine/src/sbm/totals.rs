//! Totals across different Risk Classes
use polars::prelude::*;

/// Expects three Exprs corresponding to Delta, Vega, Curvature
/// TODO check if that is fixed post 23.2
/// https://github.com/pola-rs/polars/issues/4659
pub(crate) fn total_sum3(expr: &[Expr]) -> Expr {
    apply_multiple(
        move |columns| {
            let a = unsafe{columns.get_unchecked(0)};
            let b =  unsafe{columns.get_unchecked(1)};
            let c =  unsafe{columns.get_unchecked(2)};
            a.fill_null(FillNullStrategy::Zero)?
            .add_to(&b.fill_null(FillNullStrategy::Zero)?)?
            .add_to(&c.fill_null(FillNullStrategy::Zero)?)
            
        },
        expr,
        GetOutput::from_type(DataType::Float64),
    )
}