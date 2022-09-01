//! This file defines all the measures, associated with this library

use crate::sbm::common::sens_weights;

use crate::sbm::commodity::curvature::com_curv_measures;
use crate::sbm::commodity::delta::com_delta_measures;
use crate::sbm::commodity::vega::com_vega_measures;
use crate::sbm::commodity::totals::com_total_measures;

use crate::sbm::csr_nonsec::curvature::csrnonsec_curv_measures;
use crate::sbm::csr_nonsec::delta::csrnonsec_delta_measures;
use crate::sbm::csr_nonsec::vega::csrnonsec_vega_measures;

use crate::sbm::csr_sec_ctp::curvature::csrsecctp_curv_measures;
use crate::sbm::csr_sec_ctp::delta::csrsecctp_delta_measures;
use crate::sbm::csr_sec_ctp::vega::csrsecctp_vega_measures;

use crate::sbm::csr_sec_nonctp::curvature::csrsecnonctp_curv_measures;
use crate::sbm::csr_sec_nonctp::delta::csrsecnonctp_delta_measures;
use crate::sbm::csr_sec_nonctp::vega::csrsecnonctp_vega_measures;

use crate::sbm::fx::curvature::fx_curv_measures;
use crate::sbm::fx::delta::fx_delta_measures;
use crate::sbm::fx::vega::fx_vega_measures;
use crate::sbm::fx::totals::fx_total_measures;

use crate::sbm::girr::curvature::girr_curv_measures;
use crate::sbm::girr::delta::girr_delta_measures;
use crate::sbm::girr::vega::girr_vega_measures;
use crate::sbm::girr::totals::girr_total_measures;

use crate::sbm::equity::curvature::eq_curv_measures;
use crate::sbm::equity::delta::eq_delta_measures;
use crate::sbm::equity::vega::eq_vega_measures;
use crate::sbm::equity::totals::eq_total_measures;


use base_engine::prelude::*;
//use polars::prelude::*;

/// Exporting Measures
pub(crate) fn frtb_measure_vec() -> Vec<Measure<'static>> {
    let fx_delta_measures = fx_delta_measures();
    let fx_vega_measures = fx_vega_measures();
    let fx_curv_measures = fx_curv_measures();
    let fx_total_measures = fx_total_measures();

    let com_delta_measures = com_delta_measures();
    let com_vega_measures = com_vega_measures();
    let com_curv_measures = com_curv_measures();
    let eq_delta_measures = eq_delta_measures();
    let eq_vega_measures = eq_vega_measures();
    let eq_curv_measures = eq_curv_measures();
    let csrnonsec_delta_measures = csrnonsec_delta_measures();
    let csrnonsec_vega_measures = csrnonsec_vega_measures();
    let csrnonsec_curv_measures = csrnonsec_curv_measures();
    let csrsecctp_delta_measures = csrsecctp_delta_measures();
    let csrsecctp_vega_measures = csrsecctp_vega_measures();
    let csrsecctp_curv_measures = csrsecctp_curv_measures();
    let csrsecnonctp_delta_measures = csrsecnonctp_delta_measures();
    let csrsecnonctp_vega_measures = csrsecnonctp_vega_measures();
    let csrsecnonctp_curv_measures = csrsecnonctp_curv_measures();

    let girr_delta_measures = girr_delta_measures();
    let girr_vega_measures = girr_vega_measures();
    let girr_curv_measures = girr_curv_measures();
    let girr_total_measures = girr_total_measures();

    let non_rc_specific = vec![Measure {
        name: "RiskWeights".to_string(),
        calculator: Box::new(sens_weights),
        aggregation: Some("first"),
        precomputefilter: None,
    }];

    let mut res = vec![];
    res.extend(fx_delta_measures);
    res.extend(fx_vega_measures);
    res.extend(fx_curv_measures);
    res.extend(fx_total_measures);

    res.extend(com_delta_measures);
    res.extend(com_vega_measures);
    res.extend(com_curv_measures);
    res.extend( com_total_measures() );

    res.extend(eq_delta_measures);
    res.extend(eq_vega_measures);
    res.extend(eq_curv_measures);
    res.extend( eq_total_measures() );

    res.extend(csrnonsec_delta_measures);
    res.extend(csrnonsec_vega_measures);
    res.extend(csrnonsec_curv_measures);

    res.extend(csrsecctp_delta_measures);
    res.extend(csrsecctp_vega_measures);
    res.extend(csrsecctp_curv_measures);

    res.extend(csrsecnonctp_delta_measures);
    res.extend(csrsecnonctp_vega_measures);
    res.extend(csrsecnonctp_curv_measures);

    res.extend(girr_delta_measures);
    res.extend(girr_vega_measures);
    res.extend(girr_curv_measures);
    res.extend(girr_total_measures);

    res.extend(non_rc_specific);

    res
}
