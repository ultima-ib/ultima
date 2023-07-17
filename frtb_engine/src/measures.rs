//! This file defines all the measures, associated with this library

use ultibi::{BaseMeasure, Measure};

use crate::sbm::common::sens_weights;

use crate::sbm::commodity::curvature::com_curv_measures;
use crate::sbm::commodity::delta::com_delta_measures;
use crate::sbm::commodity::totals::com_total_measures;
use crate::sbm::commodity::vega::com_vega_measures;

use crate::sbm::csr_nonsec::curvature::csrnonsec_curv_measures;
use crate::sbm::csr_nonsec::delta::csrnonsec_delta_measures;
use crate::sbm::csr_nonsec::totals::csrnonsec_total_measures;
use crate::sbm::csr_nonsec::vega::csrnonsec_vega_measures;

use crate::sbm::csr_sec_ctp::curvature::csrsecctp_curv_measures;
use crate::sbm::csr_sec_ctp::delta::csrsecctp_delta_measures;
use crate::sbm::csr_sec_ctp::totals::csrsecctp_total_measures;
use crate::sbm::csr_sec_ctp::vega::csrsecctp_vega_measures;

use crate::sbm::csr_sec_nonctp::curvature::csrsecnonctp_curv_measures;
use crate::sbm::csr_sec_nonctp::delta::csrsecnonctp_delta_measures;
use crate::sbm::csr_sec_nonctp::totals::csrsecnonctp_total_measures;
use crate::sbm::csr_sec_nonctp::vega::csrsecnonctp_vega_measures;

use crate::sbm::fx::curvature::fx_curv_measures;
use crate::sbm::fx::delta::fx_delta_measures;
use crate::sbm::fx::totals::fx_total_measures;
use crate::sbm::fx::vega::fx_vega_measures;

use crate::sbm::girr::curvature::girr_curv_measures;
use crate::sbm::girr::delta::girr_delta_measures;
use crate::sbm::girr::totals::girr_total_measures;
use crate::sbm::girr::vega::girr_vega_measures;

use crate::sbm::equity::curvature::eq_curv_measures;
use crate::sbm::equity::delta::eq_delta_measures;
use crate::sbm::equity::totals::eq_total_measures;
use crate::sbm::equity::vega::eq_vega_measures;

use crate::sbm::totals::sbm_total_measures;

use crate::drc::drc_nonsec::drc_nonsec_measures;
use crate::drc::drc_secnonctp::drc_secnonctp_measures;

use crate::drc::totals::drc_total_measures;

use crate::rrao::rrao_measures;

use crate::totals::sa_total_measures;

use crate::prelude::total_delta_sens;
use ultibi::CPM;

/// Exporting Measures
pub(crate) fn frtb_measure_vec() -> Vec<Measure> {
    let non_rc_specific = vec![
        Measure::Base(BaseMeasure {
            name: "RiskWeights".to_string(),
            calculator: std::sync::Arc::new(sens_weights),
            aggregation: Some("first".into()),
            precomputefilter: None,
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Total DeltaSens".to_string(),
            calculator: std::sync::Arc::new(|_: &CPM| Ok(total_delta_sens())),
            aggregation: None,
            precomputefilter: None,
            calc_params: vec![],
        }),
    ];

    let mut res = vec![];
    res.extend(fx_delta_measures());
    res.extend(fx_vega_measures());
    res.extend(fx_curv_measures());
    res.extend(fx_total_measures());

    res.extend(com_delta_measures());
    res.extend(com_vega_measures());
    res.extend(com_curv_measures());
    res.extend(com_total_measures());

    res.extend(eq_delta_measures());
    res.extend(eq_vega_measures());
    res.extend(eq_curv_measures());
    res.extend(eq_total_measures());

    res.extend(csrnonsec_delta_measures());
    res.extend(csrnonsec_vega_measures());
    res.extend(csrnonsec_curv_measures());
    res.extend(csrnonsec_total_measures());

    res.extend(csrsecctp_delta_measures());
    res.extend(csrsecctp_vega_measures());
    res.extend(csrsecctp_curv_measures());
    res.extend(csrsecctp_total_measures());

    res.extend(csrsecnonctp_delta_measures());
    res.extend(csrsecnonctp_vega_measures());
    res.extend(csrsecnonctp_curv_measures());
    res.extend(csrsecnonctp_total_measures());

    res.extend(girr_delta_measures());
    res.extend(girr_vega_measures());
    res.extend(girr_curv_measures());
    res.extend(girr_total_measures());

    res.extend(sbm_total_measures());

    res.extend(drc_nonsec_measures());
    res.extend(drc_secnonctp_measures());

    res.extend(drc_total_measures());

    res.extend(rrao_measures());

    res.extend(sa_total_measures());

    res.extend(non_rc_specific);

    res
}
