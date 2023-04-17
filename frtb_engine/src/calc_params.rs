use once_cell::sync::Lazy;
use ultibi::CalcParameter;
//pub(crate)static FRTB_CALC_PARAMS_MAP: Lazy<HashMap<&'static str,  &'static CalcParameter>> = Lazy::new(||{
//    FRTB_CALC_PARAMS.iter()
//        .map(|param| (param.name, param))
//        .collect()
//});

pub(crate) static FRTB_CALC_PARAMS: Lazy<Vec<CalcParameter>> = Lazy::new(|| {
    let mut res = vec![
        CalcParameter {
            name: "jurisdiction".to_string(),
            default: Some("BCBS".to_string()),
            type_hint: Some("String: BCBS/CRR2".to_string()),
        },
        CalcParameter {
            name: "reporting_ccy".to_string(),
            default: Some("USD".to_string()),
            type_hint: Some("3 digit String".to_string()),
        },
        CalcParameter {
            name: "drc_offset".to_string(),
            default: Some("true".to_string()),
            type_hint: Some("bool".to_string()),
        },
        CalcParameter {
            name: "apply_fx_curv_div".to_string(),
            default: Some("true".to_string()),
            type_hint: Some("bool".to_string()),
        },
        CalcParameter {
            name: "exotic_rrao_weight".to_string(),
            default: None,
            type_hint: Some("float".to_string()),
        },
        CalcParameter {
            name: "other_rrao_weight".to_string(),
            default: None,
            type_hint: Some("float".to_string()),
        },
        CalcParameter {
            name: "erm2_ccys".to_string(),
            default: None,
            type_hint: Some("vector of strings".to_string()),
        },
    ];

    let params = [
        (
            "com_delta_diff_cty_rho_per_bucket_base".to_string(),
            None,
            Some("vector 11".to_string()),
        ),
        (
            "com_delta_rho_diff_loc_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_delta_rho_diff_tenor_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_delta_rho_overwrite_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_vega_rho_bucket_base".to_string(),
            None,
            Some("vector 11".to_string()),
        ),
        (
            "com_base_opt_mat_vega_rho_base".to_string(),
            None,
            Some("matrix 5x5".to_string()),
        ),
        (
            "csr_nonsec_delta_diff_tenor_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_nonsec_delta_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 18.".to_string()),
        ),
        (
            "csr_nonsec_delta_diff_basis_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_nonsec_vega_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 18.".to_string()),
        ),
        (
            "csr_nonsec_opt_mat_vega_rho_base".to_string(),
            None,
            Some("matrix 5x5".to_string()),
        ),
        (
            "csr_ctp_delta_diff_tenor_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_ctp_delta_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_ctp_diff_basis_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_ctp_vega_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_ctp_opt_mat_vega_rho_base".to_string(),
            None,
            Some("matrix 5x5".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_diff_tenor_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector 25".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_diff_tranche_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "csr_sec_nonctp_vega_rho_diff_name_per_bucket_base".to_string(),
            None,
            Some("vector 25".to_string()),
        ),
        (
            "csr_sec_nonctp_opt_mat_vega_rho_base".to_string(),
            None,
            Some("matrix 5x5".to_string()),
        ),
        (
            "eq_delta_diff_name_rho_per_bucket_base".to_string(),
            None,
            Some("vector 13".to_string()),
        ),
        (
            "eq_delta_diff_type_rho_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "eq_vega_rho_diff_name_per_bucket_base".to_string(),
            None,
            Some("vector 13".to_string()),
        ),
        (
            "eq_opt_mat_vega_rho_base".to_string(),
            None,
            Some("matrix 5x5".to_string()),
        ),
        (
            "girr_delta_rho_same_curve_base".to_string(),
            None,
            Some("matrix 10x10".to_string()),
        ),
        (
            "girr_delta_rho_diff_curve_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_delta_rho_infl_base".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_delta_rho_xccy_base".to_string(),
            None,
            Some("float".to_string()),
        ),
    ];
    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name,
            default,
            type_hint,
        });
    }
    // per scenario params
    let params: [(String, Option<String>, Option<String>); 93] = [
        // HIGH
        (
            "fx_delta_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_opt_mat_vega_rho_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_vega_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_curv_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_delta_gamma_high".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_vega_gamma_high".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_gamma_high".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_diff_name_rho_per_bucket_high".to_string(),
            None,
            Some("vector 11x11".to_string()),
        ),
        (
            "csr_nonsec_delta_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_high".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 18.".to_string()),
        ),
        (
            "csr_nonsec_vega_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_delta_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_vega_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_high".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_ctp_curv_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_gamma_high".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_vega_gamma_high".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_high".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_gamma_high".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "eq_delta_gamma_high".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_vega_gamma_high".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_gamma_high".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_diff_name_rho_per_bucket_high".to_string(),
            None,
            Some("vector 13".to_string()),
        ),
        (
            "girr_delta_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_delta_gamma_erm2_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_rho_high".to_string(),
            None,
            Some("matrix 35x35".to_string()),
        ),
        (
            "girr_vega_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_gamma_erm2_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_erm2_high".to_string(),
            None,
            Some("float".to_string()),
        ),
        // Medium
        (
            "fx_delta_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_opt_mat_vega_rho_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_vega_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_curv_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_delta_gamma_medium".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_vega_gamma_medium".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_gamma_medium".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_diff_name_rho_per_bucket_medium".to_string(),
            None,
            Some("vector 11x11".to_string()),
        ),
        (
            "csr_nonsec_delta_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_medium".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 18.".to_string()),
        ),
        (
            "csr_nonsec_vega_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_delta_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_vega_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_medium".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_ctp_curv_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_gamma_medium".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_vega_gamma_medium".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_medium".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_gamma_medium".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "eq_delta_gamma_medium".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_vega_gamma_medium".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_gamma_medium".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_diff_name_rho_per_bucket_medium".to_string(),
            None,
            Some("vector 13".to_string()),
        ),
        (
            "girr_delta_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_delta_gamma_erm2_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_rho_medium".to_string(),
            None,
            Some("matrix 35x35".to_string()),
        ),
        (
            "girr_vega_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_gamma_erm2_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_erm2_medium".to_string(),
            None,
            Some("float".to_string()),
        ),
        // Low
        (
            "fx_delta_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_opt_mat_vega_rho_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_vega_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "fx_curv_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "com_delta_gamma_low".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_vega_gamma_low".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_gamma_low".to_string(),
            None,
            Some("matrix 11x11".to_string()),
        ),
        (
            "com_curv_diff_name_rho_per_bucket_low".to_string(),
            None,
            Some("vector 11x11".to_string()),
        ),
        (
            "csr_nonsec_delta_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_nonsec_curv_diff_name_rho_per_bucket_low".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 18.".to_string()),
        ),
        (
            "csr_nonsec_vega_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 18x18 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_delta_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_vega_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_ctp_curv_diff_name_rho_per_bucket_low".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_ctp_curv_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "csr_sec_nonctp_delta_gamma_low".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_vega_gamma_low".to_string(),
            None,
            Some("matrix 25x25".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_diff_name_rho_per_bucket_low".to_string(),
            None,
            Some("vector-depends on regulation. For BCBS 16.".to_string()),
        ),
        (
            "csr_sec_nonctp_curv_gamma_low".to_string(),
            None,
            Some("matrix-depends on regulation. 16x16 for BCBS.".to_string()),
        ),
        (
            "eq_delta_gamma_low".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_vega_gamma_low".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_gamma_low".to_string(),
            None,
            Some("matrix 13x13".to_string()),
        ),
        (
            "eq_curv_diff_name_rho_per_bucket_low".to_string(),
            None,
            Some("vector 13".to_string()),
        ),
        (
            "girr_delta_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_delta_gamma_erm2_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_rho_low".to_string(),
            None,
            Some("matrix 35x35".to_string()),
        ),
        (
            "girr_vega_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_vega_gamma_erm2_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_low".to_string(),
            None,
            Some("float".to_string()),
        ),
        (
            "girr_curv_gamma_erm2_low".to_string(),
            None,
            Some("float".to_string()),
        ),
    ];

    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name,
            default,
            type_hint,
        });
    }

    res
});
