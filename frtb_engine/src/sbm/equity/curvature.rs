use base_engine::prelude::OCP;
use crate::prelude::*;
//use ndarray::{Array1, Array2};
use polars::prelude::*;

pub fn eq_curv_delta (_: &OCP) -> Expr {
    curv_delta_spot("Equity")
}
/// Helper functions
pub fn eq_curv_delta_weighted(op: &OCP) -> Expr {
    eq_curv_delta(op)*col("CurvatureRiskWeight")
}
pub fn eq_cvr_down(_: &OCP) -> Expr {
    rc_cvr_spot("Equity", CVR::Down)
}
pub fn eq_cvr_up(_: &OCP) -> Expr {
    rc_cvr_spot("Equity", CVR::Up)
}
pub fn eq_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Equity", col("PnL_Up"))
}
pub fn eq_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Equity", col("PnL_Down"))    
}