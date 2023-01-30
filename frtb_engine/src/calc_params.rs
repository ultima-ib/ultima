use base_engine::CalcParameter;
use once_cell::sync::Lazy;
//pub(crate)static FRTB_CALC_PARAMS_MAP: Lazy<HashMap<&'static str,  &'static CalcParameter>> = Lazy::new(||{
//    FRTB_CALC_PARAMS.iter()
//        .map(|param| (param.name, param))
//        .collect()
//});

pub(crate) static FRTB_CALC_PARAMS: Lazy<Vec<CalcParameter>> = Lazy::new(|| {
    let mut res = vec![
        CalcParameter {
            name: "jurisdiction",
            default: Some("BCBS"),
            type_hint: Some("String: BCBS/CRR2"),
        },
        CalcParameter {
            name: "reporting_ccy",
            default: Some("USD"),
            type_hint: Some("3 digit String"),
        },
        CalcParameter {
            name: "drc_offset",
            default: Some("true"),
            type_hint: Some("bool"),
        },
        CalcParameter {
            name: "apply_fx_curv_div",
            default: Some("true"),
            type_hint: Some("bool"),
        },
        CalcParameter {
            name: "exotic_rrao_weight",
            default: None,
            type_hint: Some("float"),
        },
        CalcParameter {
            name: "other_rrao_weight",
            default: None,
            type_hint: Some("float"),
        },
        CalcParameter {
            name: "erm2_ccys",
            default: None,
            type_hint: Some("vector of strings"),
        },
    ];

    let params = [
        (
            "com_delta_diff_cty_rho_per_bucket_base",
            None,
            Some("vector 11"),
        ),
        ("com_delta_rho_diff_loc_base", None, Some("float")),
        ("com_delta_rho_diff_tenor_base", None, Some("float")),
        ("com_delta_rho_overwrite_base", None, Some("float")),
        ("com_vega_rho_bucket_base", None, Some("vector 11")),
        ("com_base_opt_mat_vega_rho_base", None, Some("matrix 5x5")),
        ("csr_nonsec_delta_diff_tenor_rho_base", None, Some("float")),
        (
            "csr_nonsec_delta_diff_name_rho_per_bucket_base",
            None,
            Some("vector-depends on regulation. For BCBS 18."),
        ),
        ("csr_nonsec_delta_diff_basis_rho_base", None, Some("float")),
        (
            "csr_nonsec_vega_diff_name_rho_per_bucket_base",
            None,
            Some("vector-depends on regulation. For BCBS 18."),
        ),
        ("csr_nonsec_opt_mat_vega_rho_base", None, Some("matrix 5x5")),
        ("csr_ctp_delta_diff_tenor_rho_base", None, Some("float")),
        (
            "csr_ctp_delta_diff_name_rho_per_bucket_base",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        ("csr_ctp_diff_basis_rho_base", None, Some("float")),
        (
            "csr_ctp_vega_diff_name_rho_per_bucket_base",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        ("csr_ctp_opt_mat_vega_rho_base", None, Some("matrix 5x5")),
        (
            "csr_sec_nonctp_delta_diff_tenor_rho_base",
            None,
            Some("float"),
        ),
        (
            "csr_sec_nonctp_delta_diff_name_rho_per_bucket_base",
            None,
            Some("vector 25"),
        ),
        (
            "csr_sec_nonctp_delta_diff_tranche_rho_base",
            None,
            Some("float"),
        ),
        (
            "csr_sec_nonctp_vega_rho_diff_name_per_bucket_base",
            None,
            Some("vector 25"),
        ),
        (
            "csr_sec_nonctp_opt_mat_vega_rho_base",
            None,
            Some("matrix 5x5"),
        ),
        (
            "eq_delta_diff_name_rho_per_bucket_base",
            None,
            Some("vector 13"),
        ),
        ("eq_delta_diff_type_rho_base", None, Some("float")),
        (
            "eq_vega_rho_diff_name_per_bucket_base",
            None,
            Some("vector 13"),
        ),
        ("eq_opt_mat_vega_rho_base", None, Some("matrix 5x5")),
        ("girr_delta_rho_same_curve_base", None, Some("matrix 10x10")),
        ("girr_delta_rho_diff_curve_base", None, Some("float")),
        ("girr_delta_rho_infl_base", None, Some("float")),
        ("girr_delta_rho_xccy_base", None, Some("float")),
    ];
    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name,
            default,
            type_hint,
        });
    }
    // per scenario params
    let params: [(&'static str, Option<&'static str>, Option<&'static str>); 93] = [
        // HIGH
        ("fx_delta_gamma_high", None, Some("float")),
        ("fx_opt_mat_vega_rho_high", None, Some("float")),
        ("fx_vega_gamma_high", None, Some("float")),
        ("fx_curv_gamma_high", None, Some("float")),
        ("com_delta_gamma_high", None, Some("matrix 11x11")),
        ("com_vega_gamma_high", None, Some("matrix 11x11")),
        ("com_curv_gamma_high", None, Some("matrix 11x11")),
        (
            "com_curv_diff_name_rho_per_bucket_high",
            None,
            Some("vector 11x11"),
        ),
        (
            "csr_nonsec_delta_gamma_high",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_gamma_high",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_high",
            None,
            Some("vector-depends on regulation. For BCBS 18."),
        ),
        (
            "csr_nonsec_vega_gamma_high",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_ctp_delta_gamma_high",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_vega_gamma_high",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_high",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_ctp_curv_gamma_high",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_sec_nonctp_delta_gamma_high",
            None,
            Some("matrix 25x25"),
        ),
        ("csr_sec_nonctp_vega_gamma_high", None, Some("matrix 25x25")),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_high",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_sec_nonctp_curv_gamma_high",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        ("eq_delta_gamma_high", None, Some("matrix 13x13")),
        ("eq_vega_gamma_high", None, Some("matrix 13x13")),
        ("eq_curv_gamma_high", None, Some("matrix 13x13")),
        (
            "eq_curv_diff_name_rho_per_bucket_high",
            None,
            Some("vector 13"),
        ),
        ("girr_delta_gamma_high", None, Some("float")),
        ("girr_delta_gamma_erm2_high", None, Some("float")),
        ("girr_vega_rho_high", None, Some("matrix 35x35")),
        ("girr_vega_gamma_high", None, Some("float")),
        ("girr_vega_gamma_erm2_high", None, Some("float")),
        ("girr_curv_gamma_high", None, Some("float")),
        ("girr_curv_gamma_erm2_high", None, Some("float")),
        // Medium
        ("fx_delta_gamma_medium", None, Some("float")),
        ("fx_opt_mat_vega_rho_medium", None, Some("float")),
        ("fx_vega_gamma_medium", None, Some("float")),
        ("fx_curv_gamma_medium", None, Some("float")),
        ("com_delta_gamma_medium", None, Some("matrix 11x11")),
        ("com_vega_gamma_medium", None, Some("matrix 11x11")),
        ("com_curv_gamma_medium", None, Some("matrix 11x11")),
        (
            "com_curv_diff_name_rho_per_bucket_medium",
            None,
            Some("vector 11x11"),
        ),
        (
            "csr_nonsec_delta_gamma_medium",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_gamma_medium",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_medium",
            None,
            Some("vector-depends on regulation. For BCBS 18."),
        ),
        (
            "csr_nonsec_vega_gamma_medium",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_ctp_delta_gamma_medium",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_vega_gamma_medium",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_medium",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_ctp_curv_gamma_medium",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_sec_nonctp_delta_gamma_medium",
            None,
            Some("matrix 25x25"),
        ),
        (
            "csr_sec_nonctp_vega_gamma_medium",
            None,
            Some("matrix 25x25"),
        ),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_medium",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_sec_nonctp_curv_gamma_medium",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        ("eq_delta_gamma_medium", None, Some("matrix 13x13")),
        ("eq_vega_gamma_medium", None, Some("matrix 13x13")),
        ("eq_curv_gamma_medium", None, Some("matrix 13x13")),
        (
            "eq_curv_diff_name_rho_per_bucket_medium",
            None,
            Some("vector 13"),
        ),
        ("girr_delta_gamma_medium", None, Some("float")),
        ("girr_delta_gamma_erm2_medium", None, Some("float")),
        ("girr_vega_rho_medium", None, Some("matrix 35x35")),
        ("girr_vega_gamma_medium", None, Some("float")),
        ("girr_vega_gamma_erm2_medium", None, Some("float")),
        ("girr_curv_gamma_medium", None, Some("float")),
        ("girr_curv_gamma_erm2_medium", None, Some("float")),
        // Low
        ("fx_delta_gamma_low", None, Some("float")),
        ("fx_opt_mat_vega_rho_low", None, Some("float")),
        ("fx_vega_gamma_low", None, Some("float")),
        ("fx_curv_gamma_low", None, Some("float")),
        ("com_delta_gamma_low", None, Some("matrix 11x11")),
        ("com_vega_gamma_low", None, Some("matrix 11x11")),
        ("com_curv_gamma_low", None, Some("matrix 11x11")),
        (
            "com_curv_diff_name_rho_per_bucket_low",
            None,
            Some("vector 11x11"),
        ),
        (
            "csr_nonsec_delta_gamma_low",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_gamma_low",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_low",
            None,
            Some("vector-depends on regulation. For BCBS 18."),
        ),
        (
            "csr_nonsec_vega_gamma_low",
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS."),
        ),
        (
            "csr_ctp_delta_gamma_low",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_vega_gamma_low",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_low",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_ctp_curv_gamma_low",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        ("csr_sec_nonctp_delta_gamma_low", None, Some("matrix 25x25")),
        ("csr_sec_nonctp_vega_gamma_low", None, Some("matrix 25x25")),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_low",
            None,
            Some("vector-depends on regulation. For BCBS 16."),
        ),
        (
            "csr_sec_nonctp_curv_gamma_low",
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS."),
        ),
        ("eq_delta_gamma_low", None, Some("matrix 13x13")),
        ("eq_vega_gamma_low", None, Some("matrix 13x13")),
        ("eq_curv_gamma_low", None, Some("matrix 13x13")),
        (
            "eq_curv_diff_name_rho_per_bucket_low",
            None,
            Some("vector 13"),
        ),
        ("girr_delta_gamma_low", None, Some("float")),
        ("girr_delta_gamma_erm2_low", None, Some("float")),
        ("girr_vega_rho_low", None, Some("matrix 35x35")),
        ("girr_vega_gamma_low", None, Some("float")),
        ("girr_vega_gamma_erm2_low", None, Some("float")),
        ("girr_curv_gamma_low", None, Some("float")),
        ("girr_curv_gamma_erm2_low", None, Some("float")),
    ];

    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name,
            default,
            type_hint,
        });
        //res.push(CalcParameter {
        //    name: medium,
        //    default: default.clone(),
        //    type_hint: type_hint.clone(),
        //});
        //res.push(CalcParameter {
        //    name: ConstStr::empty().append_str(name).append_str("_low").as_str(),
        //    default,
        //    type_hint,
        //});
    }

    res
});
