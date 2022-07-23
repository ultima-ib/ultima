//! FRTB job entry point
pub mod prelude;
mod sbm;
mod helpers;
pub mod docs;
pub mod measures;

use sbm::buckets;
use sbm::girr::delta::girr_corr_matrix;
use sbm::delta_weights::*;
use base_engine::prelude::*;

use once_cell::sync::Lazy;
use polars::prelude::*;
use ndarray::{Array2, Array1, Axis, s};
use strum::EnumString;

pub trait FRTBDataSetT {
    fn prepare(self) -> Self;
}

impl FRTBDataSetT for DataSet {
    /// prepares DataSet by pre building columns.
    /// Useful when we don't want to build columns at each request
    /// For example, we don't want to map RiskWeights at each request
    fn prepare(mut self) -> Self {

        if self.f1.height() != 0 {
            //First, identify buckets
            let mut lf1 = self.f1.lazy().with_column(buckets::sbm_buckets());
            #[cfg(feature = "CRR2")]
            if cfg!(feature = "CRR2") { lf1 = lf1.with_column(buckets::sbm_buckets_crr2())};


            // Then assign risk weights based on buckets
            lf1 = lf1.with_column(
                weights_assign(&self.build_params).alias("SensWeights")
            );
            
            // Now,  ammend weights if required. ie has to be done after main assignment of risk weights
            let mut other_cols: Vec<Expr> = vec![];
            // 21.53 Footnote 17
            let csrnonsec_covered_bond_15 = self.build_params.get("csrnonsec_covered_bond_15")
                .and_then(|s| s.parse::<bool>().ok() )
                .unwrap_or_else(||false); 

            if csrnonsec_covered_bond_15 {
                other_cols.push( 
                    when(col("RiskClass").eq(lit("CSR_nonSec")).and(col("RiskCategory").eq(lit("Delta"))).and(col("BucketBCBS").eq(lit("8"))).and(col("CoveredBondReducedWeight").eq(lit::<bool>(true))))
                    .then(Series::new("",&[0.015]).lit().list())
                    .otherwise(col("SensWeights"))
                    .alias("SensWeights"))
            };
            // If CRR2 config, we need to derive SensWeightsCRR2
            #[cfg(feature = "CRR2")]
            if cfg!(feature = "CRR2") {
                other_cols.push( weights_assign_crr2().alias("SensWeightsCRR2") )
            };
            
            if !other_cols.is_empty(){
                lf1 = lf1.with_columns(&other_cols)
            };

           // Now, we need to also ammend CRR2 weights  
            #[cfg(feature = "CRR2")]
            // Bucket 10 as per
            // https://www.eba.europa.eu/regulation-and-policy/single-rulebook/interactive-single-rulebook/108776 
            if cfg!(feature = "CRR2") & csrnonsec_covered_bond_15 {
                lf1 = lf1.with_column(
                    when(col("RiskClass").eq(lit("CSR_nonSec"))
                        .and(col("RiskCategory").eq(lit("Delta")))
                        .and(col("BucketBCBS").eq(lit("10")))
                        .and(col("CoveredBondReducedWeight").eq(lit::<bool>(true))))
                    .then(Series::new("",&[0.015]).lit().list())
                    .otherwise(col("SensWeightsCRR2"))
                    .alias("SensWeightsCRR2")
                )
            }

            self.f1 = lf1.collect().unwrap();
        }
        self
    }

    // TODO Validate:
    // all columns are present
    //
    // columns are of expected format 
    //
    // if risk class == GIRR => Risk Factor Type is not empty and one of:
    // Yield, XCCY, Inflation
    //
    // Commodity bucket can be strictly an int in 1..11 inclusive (and non empty)
    // Commodity location empty replaced with "Default_Location"
    // Equity bucket can be strictly an int in 1..13 inclusive (and non empty)
    // Equity Risk Factor Type has to be one of: EqSpot, EqRepo (and non empty)
    //
    // CSR_nonSec BCBS buckets
    // CSR_nonSec CRR2 buckets
    // if csrnonsec_covered_bond_15 == true in build config then 
    // fn validate(self) -> Self {}
}

static MEDIUM_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
        let girr_delta_rho_same_curve = girr_corr_matrix();
        let girr_delta_rho_diff_curve = girr_delta_rho_same_curve.clone()*0.999;

        //21.83
        let n_com_tenors: usize = 11;
        let base_commodity_bucket_rho = [0.55, 0.95, 0.4, 0.8, 0.6, 0.65, 0.55, 0.45, 0.15, 0.4, 0.15];

        // Tenors
        // 1 where tenors match, 0.99 otherwise
        let mut base_com_rho_tenor = Array2::from_elem((n_com_tenors, n_com_tenors), 0.99 );
        let ones = Array1::<f64>::ones(n_com_tenors);
        base_com_rho_tenor.diag_mut().assign(&ones);
        
        //21.85
        let mut com_gamma = Array2::<f64>::from_elem((10, 10), 0.2);
        let zeros = Array1::zeros(10 );
        com_gamma.diag_mut().assign(&zeros);
        let v_col = vec![0.; 10];
        let mut v_row = v_col.clone();
        v_row.push(0.);
        com_gamma.push_column(Array1::from_vec(v_col).view()).unwrap(); //unwrap to indicate something is wrong
        com_gamma.push_row(Array1::from_vec(v_row).view()).unwrap();

        //21.78
        let eq_rho_bucket: [f64; 13] = [0.15, 0.15, 0.15, 0.15, 
            0.25, 0.25, 0.25, 0.25,
            0.075, 0.125, 0., 0.8, 0.8];
        
        let eq_rho_mult = 0.999;

        // 21.80
        let mut eq_gamma = Array2::<f64>::from_elem((10, 10), 0.15);
        eq_gamma.diag_mut().assign(&Array1::<f64>::zeros(10));
        // Bucket 11
        eq_gamma.push_column(Array1::from_vec(vec![0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).view()).unwrap();
        eq_gamma.push_row(Array1::from_vec(vec![0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).view()).unwrap();
        // Bucket 12
        eq_gamma.push_column(Array1::from_vec(vec![0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.]).view()).unwrap();
        eq_gamma.push_row(Array1::from_vec(vec![0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.]).view()).unwrap();
        //Bucket 13
        eq_gamma.push_column(Array1::from_vec(vec![0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.75]).view()).unwrap();
        eq_gamma.push_row(Array1::from_vec(vec![0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0., 0.75, 0.]).view()).unwrap();

        // CSR non Sec 21.54.2 and 21.55.2
        let mut base_csr_nonsec_rho_tenor = Array2::from_elem((5, 5), 0.65 );
        let ones = Array1::<f64>::ones(5);
        base_csr_nonsec_rho_tenor.diag_mut().assign(&ones);
        let base_csr_nonsec_rho_name_bcbs = [0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.,
        0.8, 0.8];
        let base_csr_nonsec_rho_name_crr2 = [0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,];
        let base_csr_nonsec_rho_basis = 0.999;
        //21.57 BCBS
        let mut base_csr_nonsec_gamma_rating = Array2::<f64>::ones((18,18));
        let ig = [1usize,2,3,4,5,6,7,8];
        let hy_nr = [9usize,10,11,12,13,14,15];
        base_csr_nonsec_gamma_rating.indexed_iter_mut()
        .for_each(|((i, j), v)|{
            if (ig.contains(&(i+1))&hy_nr.contains(&(j+1)))|(ig.contains(&(j+1))&hy_nr.contains(&(i+1))){
                *v = 0.5
            } else if i==j{
                *v=0.
            }
        });

        let base_csr_nonsec_gamma_sector = Array2::<f64>::from_shape_vec((18, 18), vec![
            1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45,
            0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45,
            0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.05, 0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.15, 0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.20, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.00, 0.45, 0.45,
            0.00, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.00, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 0.00, 0.45, 0.45,
            1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45,
            0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45,
            0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.05, 0.20, 0.10, 0.75, 1.00, 0.20, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.15, 0.25, 0.20, 0.10, 0.75, 1.00, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.20, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.25, 0.20, 0.10, 0.75, 1.00, 0.00, 0.45, 0.45,
            0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 1.00, 0.00, 0.00,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 1.00, 0.75,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.75, 1.00
        ]).unwrap();

        // CSR non Sec CRR2 Article 325aj
        let mut base_csr_nonsec_gamma_rating_crr2 = Array2::<f64>::from_elem((20,20), 0.5);
        let credit_quality_step_1_3 = [1usize,2,3,4,5,6,7,8,9,10];
        let credit_quality_step_4_6 = [11usize,12,13,14,15,16,17];
        base_csr_nonsec_gamma_rating_crr2.indexed_iter_mut()
        .for_each(|((i, j), v)|{
            if (credit_quality_step_1_3.contains(&(i+1))&credit_quality_step_1_3.contains(&(j+1)))|(credit_quality_step_4_6.contains(&(j+1))&credit_quality_step_4_6.contains(&(i+1))){
                *v = 1.
            } else if i==j{
                *v=0.
            }
        });

        let base_csr_nonsec_gamma_sector_crr2 = Array2::<f64>::from_shape_vec((20, 20), vec![
            0.00, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45,
            1.00, 0.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 0.10, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45,
            0.75, 0.75, 0.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.10, 0.75, 1.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45,
            0.10, 0.10, 0.05, 0.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.20, 0.10, 0.05, 1.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.20, 0.15, 0.05, 0.00, 0.20, 0.25, 0.05, 0.05, 0.05, 0.20, 0.15, 0.05, 1.00, 0.20, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.25, 0.25, 0.20, 0.15, 0.20, 0.00, 0.25, 0.05, 0.15, 0.15, 0.25, 0.20, 0.15, 0.20, 1.00, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 0.00, 0.05, 0.20, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 1.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05, 0.00, 0.05, 0.05, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05, 1.00, 0.00, 0.45, 0.45,
            0.10, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 0.00, 1.00, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            0.10, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 1.00, 0.00, 0.10, 0.10, 0.20, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            1.00, 1.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.10, 0.10, 0.00, 0.75, 0.10, 0.20, 0.25, 0.20, 0.15, 0.00, 0.45, 0.45,
            0.75, 0.75, 0.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.10, 0.10, 0.75, 0.00, 0.05, 0.15, 0.20, 0.15, 0.10, 0.00, 0.45, 0.45,
            0.10, 0.10, 0.05, 0.00, 0.05, 0.15, 0.20, 0.05, 0.20, 0.20, 0.10, 0.05, 0.00, 0.05, 0.15, 0.20, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.20, 0.15, 0.05, 0.00, 0.20, 0.25, 0.05, 0.05, 0.05, 0.20, 0.15, 0.05, 0.00, 0.20, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.25, 0.25, 0.20, 0.15, 0.20, 0.00, 0.25, 0.05, 0.15, 0.15, 0.25, 0.20, 0.15, 0.20, 0.00, 0.25, 0.05, 0.00, 0.45, 0.45,
            0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 0.00, 0.05, 0.20, 0.20, 0.20, 0.15, 0.20, 0.25, 0.25, 0.00, 0.05, 0.00, 0.45, 0.45,
            0.15, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05, 0.00, 0.05, 0.05, 0.15, 0.10, 0.05, 0.05, 0.05, 0.05, 0.00, 0.00, 0.45, 0.45,
            0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.00, 0.75,
            0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.75, 0.00])
            .unwrap();


        // CSR Sec CTP 21.54.2 and 21.55.2
        let base_csr_ctp_rho_tenor = base_csr_nonsec_rho_tenor.clone();
        // TODO check what's the Rho for bucket 16 here(ie CSR sec CTP)
        let base_csr_ctp_rho_name_bcbs = [0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.8]; // <-- TODO CHECK THIS
        let base_csr_ctp_rho_name_crr2 = [0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35, 0.35, 0.35,
        0.35, 0.35, 0.35];
        let base_csr_ctp_rho_basis = 0.99;
        // CTP gamma is same nonSec, except buckets 17 and 16
        // rememebr indexing starts from 0
        let mut base_csr_ctp_gamma_rating = base_csr_nonsec_gamma_rating.clone();
        let mut base_csr_ctp_gamma_sector = base_csr_nonsec_gamma_sector.clone();
        for gamma in [&mut base_csr_ctp_gamma_rating, &mut base_csr_ctp_gamma_sector] {
            gamma.remove_index(Axis(0), 17);
            gamma.remove_index(Axis(1), 17);
            gamma.remove_index(Axis(0), 16);
            gamma.remove_index(Axis(1), 16);
        };

        //CTP CRR2
        let mut base_csr_ctp_gamma_rating_crr2 = base_csr_nonsec_gamma_rating_crr2.clone();
        let mut base_csr_ctp_gamma_sector_crr2 = base_csr_nonsec_gamma_sector_crr2.clone();
        for gamma in [&mut base_csr_ctp_gamma_rating_crr2, &mut base_csr_ctp_gamma_sector_crr2] {
            gamma.remove_index(Axis(0), 19);
            gamma.remove_index(Axis(1), 19);
            gamma.remove_index(Axis(0), 18);
            gamma.remove_index(Axis(1), 18);
        };

        // CSR Sec nonCTP 21.68
        let mut base_csr_sec_nonctp_rho_tenor = Array2::from_elem((5, 5), 0.8 );
        let ones = Array1::<f64>::ones(5);
        base_csr_sec_nonctp_rho_tenor.diag_mut().assign(&ones);
        // Tranche is CSR Sec nonCTP alternative for Name(Risk Factor)
        let base_csr_sec_nonctp_rho_diff_tranche = [
            0.4, 0.4, 0.4, 0.4, 0.4,
            0.4, 0.4, 0.4, 0.4, 0.4,
            0.4, 0.4, 0.4, 0.4, 0.4,
            0.4, 0.4, 0.4, 0.4, 0.4,
            0.4, 0.4, 0.4, 0.4, 0.,];
        let base_csr_sec_nonctp_rho_diff_basis = 0.999;

        let mut csr_sec_nonctp_gamma = Array2::<f64>::zeros((25, 25) );
        let mut base_csr_sec_nonctp_gamma_col25_slice = csr_sec_nonctp_gamma.slice_mut(s![..,24]);
        base_csr_sec_nonctp_gamma_col25_slice.fill(1.);
        let mut base_csr_sec_nonctp_gamma_row25_slice = csr_sec_nonctp_gamma.slice_mut(s![24,..]);
        base_csr_sec_nonctp_gamma_row25_slice.fill(1.);
        // Finally, all diag elements must be 0 for gamma
        let mut base_csr_sec_nonctp_gamma_row25col25_slice = csr_sec_nonctp_gamma.slice_mut(s![24,24]);
        base_csr_sec_nonctp_gamma_row25col25_slice.fill(0.);

        ScenarioConfig {
            name: ScenarioName::Medium,
            scenario_fn: med_fn,
            girr_delta_rho_same_curve,
            girr_delta_rho_diff_curve,
            girr_delta_rho_infl: 0.4,
            girr_delta_rho_xccy: 0.,
            girr_delta_gamma: 0.5,

            fx_delta_rho: 1.,
            fx_delta_gamma: 0.6,

            base_com_rho_cty: base_commodity_bucket_rho,
            base_com_rho_basis_diff: 0.999,
            base_com_rho_tenor,
            com_gamma,

            base_eq_rho_bucket: eq_rho_bucket,
            base_eq_rho_mult: eq_rho_mult,
            eq_gamma,

            //CSR nonSec
            base_csr_nonsec_rho_tenor,
            base_csr_nonsec_rho_name_bcbs,
            base_csr_nonsec_rho_basis,

            base_csr_nonsec_gamma_rating,
            base_csr_nonsec_gamma_sector,
            // CRR2 325aj
            base_csr_nonsec_rho_name_crr2,
            base_csr_nonsec_gamma_rating_crr2,
            base_csr_nonsec_gamma_sector_crr2,

            //CSR Sec CTP
            base_csr_ctp_rho_tenor,
            base_csr_ctp_rho_name_bcbs,
            base_csr_ctp_rho_basis,

            base_csr_ctp_gamma_rating,
            base_csr_ctp_gamma_sector,
            // CSR Sec CTP CRR2
            base_csr_ctp_rho_name_crr2,
            base_csr_ctp_gamma_rating_crr2,
            base_csr_ctp_gamma_sector_crr2,

            // CSR Sec nonCTP
            base_csr_sec_nonctp_rho_tenor,
            base_csr_sec_nonctp_rho_diff_tranche,
            base_csr_sec_nonctp_rho_diff_basis,

            csr_sec_nonctp_gamma

        }}
);

pub(crate) fn high_fn(x: f64) -> f64 {
    (1.25*x).min(1.)
}

pub(crate) fn med_fn(x: f64) -> f64 {
    x
}

pub(crate) fn low_fn(x: f64) -> f64 {
    (2.*x - 1.).max(0.75*x)
}

//static HIGH_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
//    MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::High, |x|(1.25*x).min(1.))
// });

static HIGH_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
    MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::High, high_fn)
 });
 static LOW_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
    MEDIUM_CORR_SCENARIO.create_scenario_from_med(ScenarioName::Low, low_fn)
 });

// Corresponds to three correlation scenarios
// 21.6
#[derive(Debug, Copy, Clone)]
pub enum ScenarioName {
    High,
    Medium,
    Low,
}

// This struct to keep all parameters, per Corr Scenario
// Weights, Gammas, Rhos etc
pub struct ScenarioConfig{
    pub name: ScenarioName,
    pub scenario_fn: fn(f64) -> f64,

    pub girr_delta_rho_same_curve: Array2<f64>,
    pub girr_delta_rho_diff_curve: Array2<f64>,
    pub girr_delta_rho_infl: f64,
    pub girr_delta_rho_xccy: f64,
    pub girr_delta_gamma: f64,
    pub fx_delta_rho: f64,
    pub fx_delta_gamma: f64,
    // Commodity rho cannot be precomputed since too many options:
    // 21.83.1 same commodity or diff commodity(depends on per bucket)
    pub base_com_rho_cty: [f64; 11],
    // 21.83.2 same or diff tenors
    pub base_com_rho_tenor: Array2<f64>,
    // 21.83.3 same or diff location
    //pub com_rho_basis_same: f64,
    pub base_com_rho_basis_diff: f64,
    // Just keep just base where index is Bucket number, value is intra-bucket rho
    // 21.85
    pub com_gamma: Array2<f64>,
    // Equity rho cannot be precomputed since depeds on RF and RFT:
    // 21.78.2
    pub base_eq_rho_bucket: [f64; 13],
    // 21.78.1 and 21.78.4
    pub base_eq_rho_mult: f64,
    // 21.80
    pub eq_gamma: Array2<f64>,
    // CSRnonSec 21.54.2 and 21.55.2
    pub base_csr_nonsec_rho_tenor: Array2<f64>,
    pub base_csr_nonsec_rho_name_bcbs: [f64; 18],
    pub base_csr_nonsec_rho_basis: f64,
    pub base_csr_nonsec_gamma_rating: Array2<f64>,
    pub base_csr_nonsec_gamma_sector: Array2<f64>,
    // CSR non Sec CRR2 325aj
    pub base_csr_nonsec_rho_name_crr2: [f64; 20],
    pub base_csr_nonsec_gamma_rating_crr2: Array2<f64>,
    pub base_csr_nonsec_gamma_sector_crr2: Array2<f64>,
    //CSR CTP
    pub base_csr_ctp_rho_tenor: Array2<f64>,
    pub base_csr_ctp_rho_name_bcbs: [f64; 16],
    pub base_csr_ctp_rho_basis: f64,
    pub base_csr_ctp_gamma_rating: Array2<f64>,
    pub base_csr_ctp_gamma_sector: Array2<f64>,

    pub base_csr_ctp_rho_name_crr2: [f64; 18],
    pub base_csr_ctp_gamma_rating_crr2: Array2<f64>,
    pub base_csr_ctp_gamma_sector_crr2: Array2<f64>,

    //CSR Sec nonCTP 21.68
    pub base_csr_sec_nonctp_rho_tenor: Array2<f64>,
    pub base_csr_sec_nonctp_rho_diff_tranche: [f64; 25],
    pub base_csr_sec_nonctp_rho_diff_basis: f64,

    pub csr_sec_nonctp_gamma: Array2<f64>,



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
    pub (crate) fn create_scenario_from_med(&self, scenario: ScenarioName, function: fn(f64)->f64) -> Self 
    //where F: Fn(f64) -> f64 + Sync,
    {
        //First, apply function to matrixes 
        let mut matrixes: [Array2<f64>; 5] = [self.girr_delta_rho_same_curve.to_owned(), self.girr_delta_rho_diff_curve.to_owned(),
        self.com_gamma.to_owned(), self.eq_gamma.to_owned(), self.csr_sec_nonctp_gamma.to_owned()];

        matrixes.iter_mut()
        .for_each(|matrix| matrix.par_mapv_inplace(|element| {function(element)})
        );
        //Unzip matrixes into individual components
        let[girr_delta_rho_same_curve, girr_delta_rho_diff_curve,
        com_gamma, eq_gamma, csr_sec_nonctp_gamma] = matrixes;

        //objects which do not implement copy
        let base_com_rho_tenor = self.base_com_rho_tenor.to_owned();

        let base_csr_nonsec_rho_tenor = self.base_csr_nonsec_rho_tenor.to_owned();
        let base_csr_nonsec_gamma_rating = self.base_csr_nonsec_gamma_rating.to_owned();
        let base_csr_nonsec_gamma_sector = self.base_csr_nonsec_gamma_sector.to_owned();
        let base_csr_nonsec_gamma_rating_crr2 = self.base_csr_nonsec_gamma_rating_crr2.to_owned();
        let base_csr_nonsec_gamma_sector_crr2 = self.base_csr_nonsec_gamma_sector_crr2.to_owned();

        let base_csr_ctp_rho_tenor = self.base_csr_ctp_rho_tenor.to_owned();
        let base_csr_ctp_gamma_rating = self.base_csr_ctp_gamma_rating.to_owned();
        let base_csr_ctp_gamma_sector = self.base_csr_ctp_gamma_sector.to_owned();

        let base_csr_ctp_gamma_rating_crr2 = self.base_csr_ctp_gamma_rating_crr2.to_owned();
        let base_csr_ctp_gamma_sector_crr2 = self.base_csr_ctp_gamma_sector_crr2.to_owned();

        let base_csr_sec_nonctp_rho_tenor = self.base_csr_sec_nonctp_rho_tenor.to_owned();

        //Next, apply to singles and return a scenario
        Self {  name: scenario,
                scenario_fn: function as fn(f64) -> f64,
                girr_delta_rho_same_curve,
                girr_delta_rho_diff_curve, 
                girr_delta_rho_infl: function(self.girr_delta_rho_infl), 
                girr_delta_rho_xccy: function(self.girr_delta_rho_xccy), 
                girr_delta_gamma: function(self.girr_delta_gamma), 

                fx_delta_rho: function(self.fx_delta_rho),
                fx_delta_gamma: function(self.fx_delta_gamma),

                base_com_rho_tenor,
                com_gamma,

                eq_gamma,

                base_csr_nonsec_rho_tenor,
                base_csr_nonsec_gamma_rating,
                base_csr_nonsec_gamma_sector,
                base_csr_nonsec_gamma_rating_crr2,
                base_csr_nonsec_gamma_sector_crr2,

                base_csr_ctp_rho_tenor,
                base_csr_ctp_gamma_rating,
                base_csr_ctp_gamma_sector,

                base_csr_ctp_gamma_rating_crr2,
                base_csr_ctp_gamma_sector_crr2,

                base_csr_sec_nonctp_rho_tenor,

                csr_sec_nonctp_gamma,

                ..*self }
        
    }
}

#[derive(Default, Debug)]
#[derive(EnumString)]
pub(crate) enum Jurisdiction {
    #[default]
    BCBS,
    #[cfg(feature = "CRR2")]
    CRR2
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}