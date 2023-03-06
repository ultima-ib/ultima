//! Defines statics of our library
//! These are mainly common types/calculations which are used throughout the library
//! which we would not want to recalculate every time due to performance concerns
use crate::drc::drc_weights;
use crate::prelude::sbm::{common::option_maturity_rho, girr::vega::girr_vega_rho};
use crate::sbm::girr::delta::girr_corr_matrix;

use ndarray::{s, Array1, Array2, Axis};
use once_cell::sync::Lazy;
use serde::Serialize;
use strum::EnumString;
use ultibi::polars::export::num::Float;
use ultibi::polars::prelude::DataFrame;

pub static MEDIUM_CORR_SCENARIO: Lazy<ScenarioConfig> = Lazy::new(|| {
    let girr_delta_rho_same_curve = girr_corr_matrix();
    let girr_delta_rho_diff_curve = 0.999;

    //21.83
    let base_com_delta_bucket_rho: [f64; 11] =
        [0.55, 0.95, 0.4, 0.8, 0.6, 0.65, 0.55, 0.45, 0.15, 0.4, 0.15];
    let base_com_curv_rho_cty: [f64; 11] = base_com_delta_bucket_rho
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    // Tenors
    // 1 where tenors match, 0.99 otherwise
    let base_com_rho_tenor = 0.99;

    //21.85
    let mut com_gamma = Array2::<f64>::from_elem((10, 10), 0.2);
    let zeros = Array1::zeros(10);
    com_gamma.diag_mut().assign(&zeros);
    let v_col = vec![0.; 10];
    let mut v_row = v_col.clone();
    v_row.push(0.);
    com_gamma
        .push_column(Array1::from_vec(v_col).view())
        .unwrap(); //unwrap to indicate something is wrong
    com_gamma.push_row(Array1::from_vec(v_row).view()).unwrap();

    let mut com_gamma_curv = com_gamma.clone();
    com_gamma_curv.mapv_inplace(|x| x.powi(2));

    //21.78
    let eq_rho_bucket: [f64; 13] = [
        0.15, 0.15, 0.15, 0.15, 0.25, 0.25, 0.25, 0.25, 0.075, 0.125, 0., 0.8, 0.8,
    ];
    let base_curv_eq_rho_bucket: [f64; 13] = eq_rho_bucket
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    let eq_rho_mult = 0.999;

    // 21.80
    let mut eq_gamma = Array2::<f64>::from_elem((10, 10), 0.15);
    eq_gamma.diag_mut().assign(&Array1::<f64>::zeros(10));
    // Bucket 11
    eq_gamma
        .push_column(Array1::from_vec(vec![0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).view())
        .unwrap();
    eq_gamma
        .push_row(Array1::from_vec(vec![0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).view())
        .unwrap();
    // Bucket 12
    eq_gamma
        .push_column(
            Array1::from_vec(vec![
                0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.,
            ])
            .view(),
        )
        .unwrap();
    eq_gamma
        .push_row(
            Array1::from_vec(vec![
                0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.,
            ])
            .view(),
        )
        .unwrap();
    //Bucket 13
    eq_gamma
        .push_column(
            Array1::from_vec(vec![
                0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.75,
            ])
            .view(),
        )
        .unwrap();
    eq_gamma
        .push_row(
            Array1::from_vec(vec![
                0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.75, 0.,
            ])
            .view(),
        )
        .unwrap();

    let mut eq_gamma_curv = eq_gamma.clone();
    eq_gamma_curv.mapv_inplace(|x| x.powi(2));

    // CSR non Sec 21.54.2 and 21.55.2
    let base_csr_nonsec_rho_tenor = 0.65;
    let base_csr_nonsec_rho_name_bcbs = [
        0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35,
        0., 0.8, 0.8,
    ];
    // 21.100.1
    let base_csr_nonsec_rho_name_bcbs_curv: [f64; 18] = base_csr_nonsec_rho_name_bcbs
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    let base_csr_nonsec_rho_name_crr2 = [
        0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
    ];
    let base_csr_nonsec_rho_name_crr2_curv: [f64; 20] = base_csr_nonsec_rho_name_crr2
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    let base_csr_nonsec_rho_basis = 0.999;
    //21.57 BCBS
    let mut base_csr_nonsec_gamma_rating = Array2::<f64>::ones((18, 18));
    let ig = [1usize, 2, 3, 4, 5, 6, 7, 8];
    let hy_nr = [9usize, 10, 11, 12, 13, 14, 15];
    base_csr_nonsec_gamma_rating
        .indexed_iter_mut()
        .for_each(|((i, j), v)| {
            if (ig.contains(&(i + 1)) & hy_nr.contains(&(j + 1)))
                | (ig.contains(&(j + 1)) & hy_nr.contains(&(i + 1)))
            {
                *v = 0.5
            } else if i == j {
                *v = 0.
            }
        });

    let base_csr_nonsec_gamma_sector = Array2::<f64>::from_shape_vec(
        (18, 18),
        vec![
            1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20,
            0.15, 0.00, 0.45, 0.45, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.75, 1.00,
            0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20,
            0.05, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45, 0.20, 0.10,
            0.75, 1.00, 0.20, 0.25, 0.05, 0.05, 0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.00,
            0.45, 0.45, 0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.15, 0.25, 0.20, 0.10, 0.75,
            1.00, 0.25, 0.05, 0.00, 0.45, 0.45, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.20,
            0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.00, 0.45, 0.45, 0.15, 0.20, 0.25, 0.20,
            0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.00, 0.45, 0.45,
            0.00, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.00, 0.15, 0.20, 0.25, 0.20, 0.10,
            0.75, 0.00, 0.45, 0.45, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 1.00, 0.75,
            0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15,
            0.10, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45, 0.10, 0.75,
            1.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00,
            0.45, 0.45, 0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.05, 0.20, 0.10, 0.75, 1.00,
            0.20, 0.25, 0.05, 0.00, 0.45, 0.45, 0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.15,
            0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.00, 0.45, 0.45, 0.20, 0.25, 0.20, 0.10,
            0.75, 1.00, 0.05, 0.20, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75,
            1.00, 0.00, 0.45, 0.45, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
            0.00, 0.00, 0.00, 0.00, 0.00, 1.00, 0.00, 0.00, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.00, 1.00, 0.75, 0.45, 0.45,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.00,
            0.75, 1.00,
        ],
    )
    .unwrap();
    let csr_nonsec_gamma = base_csr_nonsec_gamma_rating * base_csr_nonsec_gamma_sector;
    let mut csr_nonsec_gamma_curv = csr_nonsec_gamma.clone();
    csr_nonsec_gamma_curv.mapv_inplace(|x| x.powi(2));

    // CSR non Sec CRR2 Article 325aj
    let mut base_csr_nonsec_gamma_rating_crr2 = Array2::<f64>::from_elem((20, 20), 0.5);
    let credit_quality_step_1_3 = [1usize, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let credit_quality_step_4_6 = [11usize, 12, 13, 14, 15, 16, 17];
    base_csr_nonsec_gamma_rating_crr2
        .indexed_iter_mut()
        .for_each(|((i, j), v)| {
            if (credit_quality_step_1_3.contains(&(i + 1))
                & credit_quality_step_1_3.contains(&(j + 1)))
                | (credit_quality_step_4_6.contains(&(j + 1))
                    & credit_quality_step_4_6.contains(&(i + 1)))
            {
                *v = 1.
            } else if i == j {
                *v = 0.
            }
        });

    let base_csr_nonsec_gamma_sector_crr2 = Array2::<f64>::from_shape_vec(
        (20, 20),
        vec![
            0.00, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 0.10, 1.00, 0.75, 0.10, 0.20,
            0.25, 0.20, 0.15, 0.00, 0.45, 0.45, 1.00, 0.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15,
            0.10, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45, 0.75, 0.75,
            0.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15,
            0.10, 0.00, 0.45, 0.45, 0.10, 0.10, 0.05, 0.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.20,
            0.10, 0.05, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45, 0.20, 0.20, 0.15, 0.05,
            0.00, 0.20, 0.25, 0.05, 0.05, 0.05, 0.20, 0.15, 0.05, 1.00, 0.20, 0.25, 0.05, 0.00,
            0.45, 0.45, 0.25, 0.25, 0.20, 0.15, 0.20, 0.00, 0.25, 0.05, 0.15, 0.15, 0.25, 0.20,
            0.15, 0.20, 1.00, 0.25, 0.05, 0.00, 0.45, 0.45, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25,
            0.00, 0.05, 0.20, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 1.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05, 0.00, 0.05, 0.05, 0.15, 0.10, 0.05, 0.05,
            0.05, 0.05, 1.00, 0.00, 0.45, 0.45, 0.10, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05,
            0.00, 1.00, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45, 0.10, 0.10,
            0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 1.00, 0.00, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20,
            0.05, 0.00, 0.45, 0.45, 1.00, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 0.10,
            0.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45, 0.75, 0.75, 0.00, 0.05,
            0.15, 0.20, 0.15, 0.10, 0.10, 0.10, 0.75, 0.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00,
            0.45, 0.45, 0.10, 0.10, 0.05, 0.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.20, 0.10, 0.05,
            0.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45, 0.20, 0.20, 0.15, 0.05, 0.00, 0.20,
            0.25, 0.05, 0.05, 0.05, 0.20, 0.15, 0.05, 0.00, 0.20, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.25, 0.25, 0.20, 0.15, 0.20, 0.00, 0.25, 0.05, 0.15, 0.15, 0.25, 0.20, 0.15, 0.20,
            0.00, 0.25, 0.05, 0.00, 0.45, 0.45, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 0.00, 0.05,
            0.20, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 0.00, 0.05, 0.00, 0.45, 0.45, 0.15, 0.15,
            0.10, 0.05, 0.05, 0.05, 0.05, 0.00, 0.05, 0.05, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05,
            0.00, 0.00, 0.45, 0.45, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
            0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.45, 0.45, 0.45, 0.45,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.00,
            0.00, 0.75, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.00, 0.75, 0.00,
        ],
    )
    .unwrap();
    let csr_nonsec_gamma_crr2 =
        base_csr_nonsec_gamma_rating_crr2 * base_csr_nonsec_gamma_sector_crr2;
    let mut csr_nonsec_gamma_crr2_curv = csr_nonsec_gamma_crr2.clone();
    csr_nonsec_gamma_crr2_curv.mapv_inplace(|x| x.powi(2));

    // CSR Sec CTP 21.54.2 and 21.55.2
    let base_csr_ctp_rho_tenor = base_csr_nonsec_rho_tenor;
    // TODO check what's the Rho for bucket 16 here(ie CSR sec CTP)
    let base_csr_ctp_rho_name_bcbs = [
        0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35,
        0.8,
    ]; // <-- TODO CHECK THIS
    let base_csr_ctp_rho_name_bcbs_curv: [f64; 16] = base_csr_ctp_rho_name_bcbs
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    let base_csr_ctp_rho_name_crr2 = [
        0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35,
    ];
    let base_csr_ctp_rho_name_crr2_curv: [f64; 18] = base_csr_ctp_rho_name_crr2
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    // 325al
    let base_csr_ctp_rho_basis = 0.99;
    // CTP gamma is same nonSec, except buckets 17 and 16
    // rememebr indexing starts from 0
    let mut csr_ctp_gamma = csr_nonsec_gamma.clone();
    csr_ctp_gamma.remove_index(Axis(0), 17);
    csr_ctp_gamma.remove_index(Axis(1), 17);
    csr_ctp_gamma.remove_index(Axis(0), 16);
    csr_ctp_gamma.remove_index(Axis(1), 16);
    let mut csr_ctp_gamma_curv = csr_ctp_gamma.clone();
    csr_ctp_gamma_curv.mapv_inplace(|x| x.powi(2));

    //CTP CRR2
    // Buckets are same except 19 and 20
    let mut csr_ctp_gamma_crr2 = csr_nonsec_gamma_crr2.clone();
    csr_ctp_gamma_crr2.remove_index(Axis(0), 19);
    csr_ctp_gamma_crr2.remove_index(Axis(1), 19);
    csr_ctp_gamma_crr2.remove_index(Axis(0), 18);
    csr_ctp_gamma_crr2.remove_index(Axis(1), 18);
    let mut csr_ctp_gamma_crr2_curv = csr_ctp_gamma_crr2.clone();
    csr_ctp_gamma_crr2_curv.mapv_inplace(|x| x.powi(2));

    // CSR Sec nonCTP 21.68
    let base_csr_sec_nonctp_rho_tenor = 0.8;
    let base_csr_sec_nonctp_rho_diff_tranche = 0.4;
    // Although rho is same for each bucket, for convenience and flexibility we allow diff rhos for each bucket
    let base_csr_sec_nonctp_rho_diff_name: [f64; 25] = vec![0.999; 25].try_into().unwrap();
    let base_csr_sec_nonctp_rho_diff_name_curv: [f64; 25] = base_csr_sec_nonctp_rho_diff_name
        .iter()
        .map(|x| x.powi(2))
        .collect::<Vec<f64>>()
        .try_into()
        .unwrap();

    let mut csr_sec_nonctp_gamma = Array2::<f64>::zeros((25, 25));
    let mut base_csr_sec_nonctp_gamma_col25_slice = csr_sec_nonctp_gamma.slice_mut(s![.., 24]);
    base_csr_sec_nonctp_gamma_col25_slice.fill(1.);
    let mut base_csr_sec_nonctp_gamma_row25_slice = csr_sec_nonctp_gamma.slice_mut(s![24, ..]);
    base_csr_sec_nonctp_gamma_row25_slice.fill(1.);
    // Finally, all diag elements must be 0 for gamma
    let mut base_csr_sec_nonctp_gamma_row25col25_slice = csr_sec_nonctp_gamma.slice_mut(s![24, 24]);
    base_csr_sec_nonctp_gamma_row25col25_slice.fill(0.);

    let mut csr_sec_nonctp_gamma_curv = csr_sec_nonctp_gamma.clone();
    csr_sec_nonctp_gamma_curv.mapv_inplace(|x| x.powi(2));

    let base_vega_rho = option_maturity_rho();
    let fx_vega_rho = base_vega_rho.clone();

    let girr_vega_rho = girr_vega_rho();

    let drc_secnonctp_weights = drc_weights::drc_secnonctp_weights_frame();

    ScenarioConfig {
        name: ScenarioName::Medium,
        scenario_fn: med_fn,

        erm2_ccys: vec!["BGN".to_string(), "DKK".to_string(), "HRK".to_string()],
        girr_delta_rho_same_curve_base: girr_delta_rho_same_curve,
        girr_delta_rho_diff_curve_base: girr_delta_rho_diff_curve,
        girr_delta_rho_infl_base: 0.4,
        girr_delta_rho_xccy_base: 0.,
        girr_delta_vega_gamma: 0.5,
        girr_delta_vega_gamma_erm2: 0.8,
        girr_curv_gamma: 0.5f64.powi(2),
        girr_curv_gamma_erm2: 0.8f64.powi(2),

        fx_delta_vega_gamma: 0.6,
        fx_curv_gamma: 0.6f64.powi(2),

        com_delta_vega_diff_cty_rho_per_bucket_base: base_com_delta_bucket_rho,
        com_curv_diff_name_rho_per_bucket: base_com_curv_rho_cty,
        com_delta_rho_diff_loc_base: 0.999,
        com_delta_rho_diff_tenor_base: base_com_rho_tenor,
        com_delta_vega_gamma: com_gamma,
        com_curv_gamma: com_gamma_curv,

        eq_delta_vega_diff_name_rho_per_bucket_base: eq_rho_bucket,
        eq_curv_diff_name_rho_per_bucket: base_curv_eq_rho_bucket,
        eq_delta_diff_type_rho_base: eq_rho_mult,
        eq_delta_vega_gamma: eq_gamma,
        eq_curv_gamma: eq_gamma_curv,

        //CSR nonSec
        csr_nonsec_delta_diff_tenor_rho_base: base_csr_nonsec_rho_tenor,
        csr_nonsec_delta_vega_diff_name_rho_per_bucket_base_bcbs: base_csr_nonsec_rho_name_bcbs,
        csr_nonsec_curv_diff_name_rho_per_bucket_bcbs: base_csr_nonsec_rho_name_bcbs_curv,
        csr_nonsec_delta_diff_basis_rho_base: base_csr_nonsec_rho_basis,

        csr_nonsec_delta_vega_gamma_bcbs: csr_nonsec_gamma,
        csr_nonsec_curv_gamma_bcbs: csr_nonsec_gamma_curv,
        // CRR2 325aj
        csr_nonsec_delta_diff_name_rho_per_bucket_base_crr2: base_csr_nonsec_rho_name_crr2,
        csr_nonsec_curv_diff_name_rho_per_bucket_crr2: base_csr_nonsec_rho_name_crr2_curv,
        csr_nonsec_delta_vega_gamma_crr2: csr_nonsec_gamma_crr2,
        csr_nonsec_curv_gamma_crr2: csr_nonsec_gamma_crr2_curv,

        //CSR Sec CTP
        csr_ctp_delta_diff_tenor_rho_base: base_csr_ctp_rho_tenor,
        csr_ctp_delta_vega_diff_name_rho_per_bucket_base_bcbs: base_csr_ctp_rho_name_bcbs,
        csr_ctp_curv_diff_name_rho_per_bucket_bcbs: base_csr_ctp_rho_name_bcbs_curv,
        csr_ctp_diff_basis_rho_base: base_csr_ctp_rho_basis,

        csr_ctp_delta_vega_gamma_bcbs: csr_ctp_gamma,
        csr_ctp_curv_gamma_bcbs: csr_ctp_gamma_curv,
        // CSR Sec CTP CRR2
        csr_ctp_delta_vega_diff_name_rho_per_bucket_base_crr2: base_csr_ctp_rho_name_crr2,
        csr_ctp_curv_diff_name_rho_per_bucket_crr2: base_csr_ctp_rho_name_crr2_curv,
        csr_ctp_delta_vega_gamma_crr2: csr_ctp_gamma_crr2,
        csr_ctp_curv_gamma_crr2: csr_ctp_gamma_crr2_curv,

        // CSR Sec nonCTP
        csr_sec_nonctp_delta_diff_tenor_rho_base: base_csr_sec_nonctp_rho_tenor,
        csr_sec_nonctp_delta_diff_tranche_rho_base: base_csr_sec_nonctp_rho_diff_tranche,
        csr_sec_nonctp_delta_vega_diff_name_rho_per_bucket_base: base_csr_sec_nonctp_rho_diff_name,
        csr_sec_nonctp_curv_diff_name_rho_per_bucket: base_csr_sec_nonctp_rho_diff_name_curv,

        csr_sec_nonctp_delta_vega_gamma: csr_sec_nonctp_gamma,
        csr_sec_nonctp_curv_gamma: csr_sec_nonctp_gamma_curv,

        // Vega
        base_vega_rho,
        fx_opt_mat_vega_rho: fx_vega_rho,
        girr_vega_rho,

        //DRC
        drc_secnonctp_weights,

        //RRAO
        exotic: 0.01,
        other: 0.001,
    }
});

pub(crate) fn high_fn(x: f64) -> f64 {
    (1.25 * x).min(1.)
}

pub(crate) fn med_fn(x: f64) -> f64 {
    x
}

pub(crate) fn low_fn(x: f64) -> f64 {
    (2. * x - 1.).max(0.75 * x)
}

//static HIGH_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
//    MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::High, |x|(1.25*x).min(1.))
// });

pub static HIGH_CORR_SCENARIO: Lazy<ScenarioConfig> =
    Lazy::new(|| MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::High, high_fn));
pub static LOW_CORR_SCENARIO: Lazy<ScenarioConfig> =
    Lazy::new(|| MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::Low, low_fn));

// Corresponds to three correlation scenarios
// 21.6
#[derive(Debug, Copy, Clone, Serialize)]
pub enum ScenarioName {
    High,
    Medium,
    Low,
}
#[derive(Serialize)]
// This struct to keep all parameters, per Corr Scenario
// Weights, Gammas, Rhos etc
pub struct ScenarioConfig {
    pub name: ScenarioName,
    #[serde(skip)]
    pub scenario_fn: fn(f64) -> f64,

    pub erm2_ccys: Vec<String>,
    pub girr_delta_rho_same_curve_base: Array2<f64>,
    pub girr_delta_rho_diff_curve_base: f64,
    pub girr_delta_rho_infl_base: f64,
    pub girr_delta_rho_xccy_base: f64,
    pub girr_delta_vega_gamma: f64,
    pub girr_delta_vega_gamma_erm2: f64,
    pub girr_curv_gamma: f64,
    pub girr_curv_gamma_erm2: f64,

    pub fx_delta_vega_gamma: f64,
    pub fx_curv_gamma: f64,

    // Commodity rho cannot be precomputed since too many options:
    /// 21.83.1 same commodity or diff commodity(depends on per bucket)
    pub com_delta_vega_diff_cty_rho_per_bucket_base: [f64; 11],
    /// 21.101
    pub com_curv_diff_name_rho_per_bucket: [f64; 11],
    /// 21.83.2 same or diff tenors
    pub com_delta_rho_diff_tenor_base: f64,
    /// 21.83.3 same or diff location
    pub com_delta_rho_diff_loc_base: f64,
    // Just keep just base where index is Bucket number, value is intra-bucket rho
    /// 21.85
    pub com_delta_vega_gamma: Array2<f64>,
    pub com_curv_gamma: Array2<f64>,

    // Equity rho cannot be precomputed since depeds on RF and RFT:
    // 21.78.2
    pub eq_delta_vega_diff_name_rho_per_bucket_base: [f64; 13],
    ///21.100
    pub eq_curv_diff_name_rho_per_bucket: [f64; 13],
    /// 21.78.1 and 21.78.4
    /// multiplier used when rft is not equal(spot vs repo)
    pub eq_delta_diff_type_rho_base: f64,
    /// 21.80
    pub eq_delta_vega_gamma: Array2<f64>,
    /// 21.101
    pub eq_curv_gamma: Array2<f64>,

    // CSRnonSec 21.54.2 and 21.55.2
    pub csr_nonsec_delta_diff_tenor_rho_base: f64,
    pub csr_nonsec_delta_vega_diff_name_rho_per_bucket_base_bcbs: [f64; 18],
    ///21.100
    pub csr_nonsec_curv_diff_name_rho_per_bucket_bcbs: [f64; 18],
    pub csr_nonsec_delta_diff_basis_rho_base: f64,
    pub csr_nonsec_delta_vega_gamma_bcbs: Array2<f64>,
    pub csr_nonsec_curv_gamma_bcbs: Array2<f64>,

    /// CSR non Sec CRR2 325aj
    pub csr_nonsec_delta_diff_name_rho_per_bucket_base_crr2: [f64; 20],
    /// 325ay 5
    pub csr_nonsec_curv_diff_name_rho_per_bucket_crr2: [f64; 20],
    pub csr_nonsec_delta_vega_gamma_crr2: Array2<f64>,
    pub csr_nonsec_curv_gamma_crr2: Array2<f64>,

    //CSR Sec CTP
    pub csr_ctp_delta_diff_tenor_rho_base: f64,
    pub csr_ctp_delta_vega_diff_name_rho_per_bucket_base_bcbs: [f64; 16],
    pub csr_ctp_curv_diff_name_rho_per_bucket_bcbs: [f64; 16],
    pub csr_ctp_diff_basis_rho_base: f64,
    pub csr_ctp_delta_vega_gamma_bcbs: Array2<f64>,
    pub csr_ctp_curv_gamma_bcbs: Array2<f64>,

    pub csr_ctp_delta_vega_diff_name_rho_per_bucket_base_crr2: [f64; 18],
    pub csr_ctp_curv_diff_name_rho_per_bucket_crr2: [f64; 18],
    pub csr_ctp_delta_vega_gamma_crr2: Array2<f64>,
    pub csr_ctp_curv_gamma_crr2: Array2<f64>,

    //CSR Sec nonCTP 21.68
    pub csr_sec_nonctp_delta_diff_tenor_rho_base: f64,
    /// Although rho is same for each bucket, for convenience and flexibility we allow diff rhos for each bucket
    pub csr_sec_nonctp_delta_vega_diff_name_rho_per_bucket_base: [f64; 25],
    pub csr_sec_nonctp_curv_diff_name_rho_per_bucket: [f64; 25],
    pub csr_sec_nonctp_delta_diff_tranche_rho_base: f64,

    pub csr_sec_nonctp_delta_vega_gamma: Array2<f64>,
    pub csr_sec_nonctp_curv_gamma: Array2<f64>,

    //Vega
    /// Option Maturity Rho
    pub base_vega_rho: Array2<f64>,
    pub girr_vega_rho: Array2<f64>,
    /// Similar to Option Maturity Rho, but we apply scenario fn to it
    pub fx_opt_mat_vega_rho: Array2<f64>,

    //DRC
    pub drc_secnonctp_weights: DataFrame,

    //RRAO
    pub exotic: f64,
    pub other: f64,
}

impl ScenarioConfig {
    pub fn as_str(&self) -> &str {
        match self.name {
            ScenarioName::High => "_high",
            ScenarioName::Medium => "_medium",
            ScenarioName::Low => "_low",
        }
    }
}

impl ScenarioConfig {
    pub(crate) fn create_scenario_from_med(
        &self,
        scenario: ScenarioName,
        function: fn(f64) -> f64,
    ) -> Self
//where F: Fn(f64) -> f64 + Sync,
    {
        //First, apply function to matrixes
        let mut matrixes: [Array2<f64>; 16] = [
            self.com_delta_vega_gamma.to_owned(),
            self.com_curv_gamma.to_owned(),
            self.eq_delta_vega_gamma.to_owned(),
            self.csr_sec_nonctp_delta_vega_gamma.to_owned(),
            self.girr_vega_rho.to_owned(),
            self.fx_opt_mat_vega_rho.to_owned(),
            self.eq_curv_gamma.to_owned(),
            self.csr_nonsec_delta_vega_gamma_bcbs.to_owned(),
            self.csr_nonsec_delta_vega_gamma_crr2.to_owned(),
            self.csr_ctp_delta_vega_gamma_bcbs.to_owned(),
            self.csr_ctp_delta_vega_gamma_crr2.to_owned(),
            self.csr_nonsec_curv_gamma_bcbs.to_owned(),
            self.csr_nonsec_curv_gamma_crr2.to_owned(),
            self.csr_ctp_curv_gamma_bcbs.to_owned(),
            self.csr_ctp_curv_gamma_crr2.to_owned(),
            self.csr_sec_nonctp_curv_gamma.to_owned(),
        ];

        matrixes
            .iter_mut()
            .for_each(|matrix| matrix.par_mapv_inplace(function));
        //Unzip matrixes into individual components
        let [com_gamma, com_gamma_curv, eq_gamma, csr_sec_nonctp_gamma, girr_vega_rho, fx_vega_rho, eq_gamma_curv, csr_nonsec_gamma, csr_nonsec_gamma_crr2, csr_ctp_gamma, csr_ctp_gamma_crr2, csr_nonsec_gamma_curv, csr_nonsec_gamma_crr2_curv, csr_ctp_gamma_curv, csr_ctp_gamma_crr2_curv, csr_sec_nonctp_gamma_curv] =
            matrixes;

        let mut eq_curv_rho_bucket: [f64; 13] = self.eq_curv_diff_name_rho_per_bucket;
        eq_curv_rho_bucket.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut com_curv_rho_cty: [f64; 11] = self.com_curv_diff_name_rho_per_bucket;
        com_curv_rho_cty.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut csr_nonsec_rho_name_bcbs_curv = self.csr_nonsec_curv_diff_name_rho_per_bucket_bcbs;
        csr_nonsec_rho_name_bcbs_curv.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut csr_nonsec_rho_name_crr2_curv = self.csr_nonsec_curv_diff_name_rho_per_bucket_crr2;
        csr_nonsec_rho_name_crr2_curv.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut csr_ctp_rho_name_bcbs_curv = self.csr_ctp_curv_diff_name_rho_per_bucket_bcbs;
        csr_ctp_rho_name_bcbs_curv.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut csr_ctp_rho_name_crr2_curv = self.csr_ctp_curv_diff_name_rho_per_bucket_crr2;
        csr_ctp_rho_name_crr2_curv.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        let mut csr_sec_nonctp_rho_diff_name_curv =
            self.csr_sec_nonctp_curv_diff_name_rho_per_bucket;
        csr_sec_nonctp_rho_diff_name_curv.iter_mut().for_each(|x| {
            *x = function(*x);
        });

        //objects which do not implement copy
        let base_vega_rho = self.base_vega_rho.clone();
        let base_girr_delta_rho_same_curve = self.girr_delta_rho_same_curve_base.to_owned();
        let erm2_crr2 = self.erm2_ccys.clone();
        let drc_secnonctp_weights = self.drc_secnonctp_weights.clone();

        //Next, apply to singles and return a scenario
        Self {
            name: scenario,
            scenario_fn: function as fn(f64) -> f64,

            erm2_ccys: erm2_crr2,

            girr_delta_rho_same_curve_base: base_girr_delta_rho_same_curve,
            girr_delta_vega_gamma: function(self.girr_delta_vega_gamma),
            girr_curv_gamma: function(self.girr_curv_gamma),
            girr_curv_gamma_erm2: function(self.girr_curv_gamma_erm2),

            fx_delta_vega_gamma: function(self.fx_delta_vega_gamma),
            fx_curv_gamma: function(self.fx_curv_gamma),

            com_curv_diff_name_rho_per_bucket: com_curv_rho_cty,
            com_delta_vega_gamma: com_gamma,
            com_curv_gamma: com_gamma_curv,

            eq_curv_diff_name_rho_per_bucket: eq_curv_rho_bucket,
            eq_delta_vega_gamma: eq_gamma,
            eq_curv_gamma: eq_gamma_curv,

            csr_nonsec_curv_diff_name_rho_per_bucket_bcbs: csr_nonsec_rho_name_bcbs_curv,
            csr_nonsec_curv_diff_name_rho_per_bucket_crr2: csr_nonsec_rho_name_crr2_curv,
            csr_nonsec_delta_vega_gamma_bcbs: csr_nonsec_gamma,
            csr_nonsec_curv_gamma_bcbs: csr_nonsec_gamma_curv,
            csr_nonsec_delta_vega_gamma_crr2: csr_nonsec_gamma_crr2,
            csr_nonsec_curv_gamma_crr2: csr_nonsec_gamma_crr2_curv,

            csr_ctp_curv_diff_name_rho_per_bucket_bcbs: csr_ctp_rho_name_bcbs_curv,
            csr_ctp_curv_diff_name_rho_per_bucket_crr2: csr_ctp_rho_name_crr2_curv,
            csr_ctp_delta_vega_gamma_bcbs: csr_ctp_gamma,
            csr_ctp_curv_gamma_bcbs: csr_ctp_gamma_curv,
            csr_ctp_delta_vega_gamma_crr2: csr_ctp_gamma_crr2,
            csr_ctp_curv_gamma_crr2: csr_ctp_gamma_crr2_curv,

            csr_sec_nonctp_curv_diff_name_rho_per_bucket: csr_sec_nonctp_rho_diff_name_curv,
            csr_sec_nonctp_delta_vega_gamma: csr_sec_nonctp_gamma,
            csr_sec_nonctp_curv_gamma: csr_sec_nonctp_gamma_curv,

            base_vega_rho,
            fx_opt_mat_vega_rho: fx_vega_rho,
            girr_vega_rho,

            drc_secnonctp_weights,

            ..*self
        }
    }
}

#[derive(Default, Debug, EnumString)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) enum Jurisdiction {
    #[default]
    BCBS,
    #[cfg(feature = "CRR2")]
    CRR2,
}
