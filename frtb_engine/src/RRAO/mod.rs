//! RRAO module
//! RRAO is calculated at Trade Level - hence we usually need to drop duplicates
//! col("EXOTIC_RRAO"),
//col("Notional"),
//col("TradeId"),

use base_engine::{OCP, col};
use ndarray::Array1;
use polars::{prelude::{apply_multiple, Expr, GetOutput, DataType, NamedFrom, UniqueKeepStrategy}, 
    df, series::Series};
use crate::prelude::get_optional_parameter;
use crate::statics::MEDIUM_CORR_SCENARIO;
/*
pub(crate) fn exotic_notional (op: &OCP) -> Expr {
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
            
        ],
        GetOutput::from_type(DataType::Float64),
    )
}
*/
pub(crate) fn rrao(op: &OCP) -> Expr {
    let exotic_weight = get_optional_parameter(
        op,
        "exotic_rrao_weight",
        &MEDIUM_CORR_SCENARIO.exotic,
    );
    let other_weight = get_optional_parameter(
        op,
        "other_rrao_weight",
        &MEDIUM_CORR_SCENARIO.other,
    );
    rrao_charge(exotic_weight, other_weight)
}

pub(crate) fn rrao_charge (exotic_weight: f64, other_weight: f64) -> Expr {
    apply_multiple(|columns|{
        let mut df = df![
                "id"   => &columns[0],
                "e"    => &columns[1],
                "o"    => &columns[2],
                "n"    => &columns[3],
            ]?;

        let res_len = columns[0].len();

        df = df.unique(Some(&["id".to_string()]), UniqueKeepStrategy::First)?;
        
        return Ok(Series::new(
                "res",
                Array1::<f64>::from_elem(res_len, 0.)
                    .as_slice()
                    .unwrap(),
            ));
    },
    &[col("TradeId"), col("EXOTIC_RRAO"), col("OTHER_RRAO"), col("Notional")],
    GetOutput::from_type(DataType::Float64))
}
