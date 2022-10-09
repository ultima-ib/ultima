use base_engine::CalcParameter;

pub(crate) fn frtb_calc_params() -> Vec<CalcParameter> {
    let mut res = vec![];

    res.push(CalcParameter {
        name: "jurisdiction".into(),
        default: Some("BCBS".into()),
        type_hint: Some("String".into()),
    });
    res.push(CalcParameter {
        name: "drc_offset".into(),
        default: Some("true".into()),
        type_hint: Some("bool".into()),
    });
    res.push(CalcParameter {
        name: "apply_fx_curv_div".into(),
        default: Some("true".into()),
        type_hint: Some("bool".into()),
    });
    res.push(CalcParameter {
        name: "exotic_rrao_weight".into(),
        default: None,
        type_hint: Some("float".into()),
    });
    res.push(CalcParameter {
        name: "other_rrao_weight".into(),
        default: None,
        type_hint: Some("float".into()),
    });
    //res.push(CalcParameter{name: "apply_fx_curv_div".into(), default: Some("true".into()), type_hint: Some("bool".into())});

    let params = [
        ("com_delta_rho_bucket_base", None, Some("vector 11".into())),
        ("com_delta_rho_diff_loc_base", None, Some("float".into())),
        ("com_delta_rho_diff_tenor_base", None, Some("float".into())),
        ("com_delta_rho_overwrite_base", None, Some("float".into())),
        ("com_vega_rho_bucket_base", None, Some("vector 11".into())),
        ("com_base_opt_mat_vega_rho_base", None, Some("matrix 5x5".into())),
        
        ("csr_nonsec_delta_diff_tenor_rho_base", None, Some("float".into())),
        ("csr_nonsec_delta_diff_name_rho_per_bucket_base", None, Some("vector-depends on regulation. For BCBS 18.".into())),
        ("csr_nonsec_delta_diff_basis_rho_base", None, Some("float".into())),
        ("csr_nonsec_vega_diff_name_rho_per_bucket_base", None, Some("vector-depends on regulation. For BCBS 18.".into())),
        ("csr_nonsec_opt_mat_vega_rho_base", None, Some("matrix 5x5".into())),

        ("csr_ctp_tenor_rho_base", None, Some("float".into())),
        ("csr_ctp_diff_name_rho_per_bucket_base", None, Some("vector-depends on regulation. For BCBS 16.".into())),
        ("csr_ctp_diff_basis_rho_base", None, Some("float".into())),
        ("csr_ctp_vega_rho_diff_name_per_bucket_base", None, Some("vector-depends on regulation. For BCBS 16.".into())),
        ("csr_ctp_opt_mat_vega_rho_base", None, Some("matrix 5x5".into())),

        ("csr_sec_nonctp_delta_diff_tenor_rho_base", None, Some("float".into())),
        ("csr_sec_nonctp_delta_diff_name_rho_per_bucket_base", None, Some("vector 25".into())),
        ("csr_sec_nonctp_delta_diff_tranche_rho_base", None, Some("float".into())),
        ("csr_sec_nonctp_vega_rho_diff_name_per_bucket_base", None, Some("vector 25".into())),
        ("csr_sec_nonctp_opt_mat_vega_rho_base", None, Some("matrix 5x5".into())),


    ];
    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name: name.into(),
            default: default.clone(),
            type_hint: type_hint.clone(),
        });
    }
    // scenario params
    let params = [
        ("fx_delta_gamma", None, Some("float".into())),
        ("fx_vega_rho", None, Some("float".into())),
        ("fx_vega_gamma", None, Some("float".into())),
        ("fx_curv_gamma", None, Some("float".into())),

        ("com_delta_gamma", None, Some("matrix 11x11".into())),
        ("com_vega_gamma", None, Some("matrix 11x11".into())),
        ("com_curv_gamma", None, Some("matrix 11x11".into())),
        ("commodity_curv_diff_name_rho_per_bucket", None, Some("vector 11x11".into())),

        ("csr_nonsec_delta_gamma", None, Some("matrix-depends on regulation. 18x18 for BCBS.".into())),
        ("csr_nonsec_curv_gamma", None, Some("matrix-depends on regulation. 18x18 for BCBS.".into())),
        ("csr_nonsec_curv_diff_name_rho_per_bucket", None, Some("vector-depends on regulation. For BCBS 18.".into())),
        ("csr_nonsec_vega_gamma", None, Some("matrix-depends on regulation. 18x18 for BCBS.".into())),

        ("csr_ctp_delta_gamma", None, Some("matrix-depends on regulation. 16x16 for BCBS.".into())),
        ("csr_ctp_vega_gamma", None, Some("matrix-depends on regulation. 16x16 for BCBS.".into())),
        ("csr_ctp_curv_diff_name_rho_per_bucket", None, Some("vector-depends on regulation. For BCBS 16.".into())),
        ("csr_ctp_curv_gamma", None, Some("matrix-depends on regulation. 16x16 for BCBS.".into())),

        ("csr_sec_nonctp_delta_gamma", None, Some("matrix 25x25".into())),
        ("csr_sec_nonctp_vega_gamma", None, Some("matrix 25x25".into())),
        ("csr_sec_nonctp_curv_diff_name_rho_per_bucket", None, Some("vector-depends on regulation. For BCBS 16.".into())),
        ("csr_sec_nonctp_curv_gamma", None, Some("matrix-depends on regulation. 16x16 for BCBS.".into())),

        ];

    for (name, default, type_hint) in params {
        res.push(CalcParameter {
            name: format!("{}_high", name),
            default: default.clone(),
            type_hint: type_hint.clone(),
        });
        res.push(CalcParameter {
            name: format!("{}_medium", name),
            default: default.clone(),
            type_hint: type_hint.clone(),
        });
        res.push(CalcParameter {
            name: format!("{}_low", name),
            default,
            type_hint,
        });
    }

    res
}
