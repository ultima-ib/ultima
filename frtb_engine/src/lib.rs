//! FRTB job entry point
#![allow(clippy::unnecessary_lazy_evaluations)]

pub mod prelude;
mod sbm;
mod helpers;
pub mod docs;
pub mod measures;
mod statics;

use prelude::frtb_measure_vec;
use sbm::buckets;
use sbm::risk_weights::*;
use base_engine::prelude::*;

use polars::prelude::*;
use std::collections::HashMap;

pub trait FRTBDataSetT {
    fn prepare(self) -> Self;
}
#[derive(Debug)]
pub struct FRTBDataSet<'a> {
    frames: Vec<DataFrame>,
    measures: MM<'a>,  
    build_params: HashMap<String, String>,
}
impl<'a> FRTBDataSet<'a> {
    
    fn with_measures(&mut self, bespoke: Vec<Measure<'static>>)
    where Self: Sized, {
        let self_measures = &mut self.measures;
        self_measures
       .extend(bespoke.into_iter()
            .map(|m|(m.name.clone(), m))
        );
       //self
    }
}

impl<'a> DataSet for FRTBDataSet<'a> {
    fn frames(&self) -> &Vec<DataFrame>{
        &self.frames
    }
    fn measures(&self) -> &MM{
        &self.measures
    }
    
    fn build(conf: DataSourceConfig) -> Self{
        let (frames, measure_cols, build_params) = conf.build();
        let mm: MM = derive_measure_map(measure_cols);
        let mut res = Self{frames, measures: mm, build_params};
        res.with_measures(frtb_measure_vec());
        res
    }
    /// prepares DataSet by pre building columns.
    /// Useful when we don't want to build columns at each request
    /// For example, we don't want to map RiskWeights at each request
    fn prepare(&mut self) {

        let f1 = &mut self.frames[0];

        if f1.height() != 0 {
            //First, identify buckets
            let mut lf1 = f1.clone().lazy().with_column(buckets::sbm_buckets(&self.build_params));
            // If CRR2, then also provide CRR2 buckets
            #[cfg(feature = "CRR2")]
            if cfg!(feature = "CRR2") { lf1 = lf1.with_column(buckets::sbm_buckets_crr2())};

            

            // Then assign risk weights based on buckets
            lf1 = lf1.with_column( weights_assign(&self.build_params).alias("SensWeights") )
                // Curvature risk weight
                .with_column( when(col("PnL_Up").is_not_null().or(col("PnL_Down").is_not_null()))
                        .then(col("SensWeights").arr().max().alias("CurvatureRiskWeight"))
                        .otherwise(NULL.lit())
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
           // Bucket 10 as per
           // https://www.eba.europa.eu/regulation-and-policy/single-rulebook/interactive-single-rulebook/108776 
           #[cfg(feature = "CRR2")]
            if cfg!(feature = "CRR2") & csrnonsec_covered_bond_15 {
                lf1 = lf1.with_columns([
                    when(col("RiskClass").eq(lit("CSR_nonSec"))
                        .and(col("RiskCategory").eq(lit("Delta")))
                        .and(col("BucketCRR2").eq(lit("10")))
                        .and(col("CoveredBondReducedWeight").eq(lit::<bool>(true)))
                    )
                    .then(Series::new("",&[0.015]).lit().list())
                    .otherwise(col("SensWeightsCRR2"))
                    .alias("SensWeightsCRR2"),

                    col("SensWeightsCRR2").arr().max().alias("CurvatureRiskWeightCRR2")
                ])
            }

            // Have to collect into a tmp df, as the code panics otherwise
            let tmp_frame = lf1.collect().expect("Failed to unwrap while .prepare()");
            let lf2 = tmp_frame.lazy().with_column(
                when(col("RiskClass").eq(lit("GIRR"))
                        .and(col("RiskCategory").eq(lit("Vega"))))

                    .then(col("GirrVegaUnderlyingMaturity").fill_null(col("RiskFactorType")))

                    .otherwise(NULL.lit())
            );
            let tmp2_frame = lf2.collect().unwrap();
            
            *f1 = tmp2_frame;
        }
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
    // Equity Risk Factor Type has to be one of: EqSpot, EqRepo (and non empty) for Delta
    //
    // CSR_nonSec BCBS buckets
    // CSR_nonSec CRR2 buckets
    // if csrnonsec_covered_bond_15 == true in build config then 
    // fn validate(self) -> Self {}
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}