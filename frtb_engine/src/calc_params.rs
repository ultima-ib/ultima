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
    //res.push(CalcParameter{name: "apply_fx_curv_div".into(), default: Some("true".into()), type_hint: Some("bool".into())});

    // scenario params
    let params = [
        ("fx_delta_gamma", None, Some("f64".into())),
        ("fx_vega_rho", None, Some("f64".into())),
        ("fx_vega_gamma", None, Some("f64".into())),
        ("fx_curv_gamma", None, Some("f64".into())),
        ("com_delta_gamma", None, Some("matrix 11x11".into())),
        ("com_delta_rho_bucket", None, Some("vector 11".into())),
        ("com_delta_rho_diff_loc", None, Some("f64".into())),
        ("com_delta_rho_diff_tenor", None, Some("f64".into())),
        ("com_delta_rho_overwrite", None, Some("RhoOverwrite".into())),
        ("com_vega_gamma", None, Some("matrix 11x11".into())),
        ("com_base_vega_rho_bucket", None, Some("vector 11".into())),
        ("com_base_opt_mat_vega_rho", None, Some("matrix 5x5".into())),
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
