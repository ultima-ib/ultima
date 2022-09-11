use crate::prelude::*;
use base_engine::prelude::*;

use polars::prelude::*;


pub(crate) fn drc_secnonctp_grossjtd(_: &OCP) -> Expr {
    rc_sens("DRC_SecNonCTP", col("GrossJTD"))
}
pub(crate) fn drc_secnonctp_grossjtd_scaled(_: &OCP) -> Expr {
    rc_sens("DRC_SecNonCTP", col("GrossJTD")*col("ScaleFactor"))
}

pub(crate) fn drc_secnonctp_charge(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::CapitalCharge)
}
pub(crate) fn drc_secnonctp_netlongjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::NetLongJTD)
}
pub(crate) fn drc_secnonctp_netshortjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::NetShortJTD)
}
pub(crate) fn drc_secnonctp_weightednetlongjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetLongJTD)
}
pub(crate) fn drc_secnonctp_weightednetabsshortjtd(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::WeightedNetAbsShortJTD)
}
pub(crate) fn drc_secnonctp_hbr(op: &OCP) -> Expr {
    drc_secnonctp_distributor(op, ReturnMetric::HBR)
}

fn drc_secnonctp_distributor(
    op: &OCP,
    rtrn: ReturnMetric,
) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    drc_secnonctp_charge_calculator(rtrn, false)
}

/// DRC Sec Non CTP Offsetting (22.30) is not implemented yet
fn drc_secnonctp_charge_calculator(rtrn: ReturnMetric, offset: bool) -> Expr {
    todo!()
}

pub(crate) fn drc_secnonctp_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "DRC_SecNonCTP_GrossJTD".to_string(),
            calculator: Box::new(drc_secnonctp_grossjtd),
            aggregation: None,
            precomputefilter: Some(
                col("RiskClass")
                    .eq(lit("DRC_SecNonCTP"))
            ),
        },
    ]
}