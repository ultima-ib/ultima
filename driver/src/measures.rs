use std::collections::HashMap;
use polars::prelude::*;
use base_engine::prelude::MCMF;

// function derives bespoke measure maps to column and to function
// as we potentially have multiple bespoke configs
pub(crate) fn derive_bespoke_measures(mut dataset_numer_cols: Vec<String>) -> MCMF {

    let mut bespoke_measure_col_map: HashMap<String, Vec<String>> 
    = HashMap::new();
    let mut bespoke_measure_fn_map: HashMap<String, fn() -> Expr> 
    = HashMap::new();

        //#[cfg(feature = "FRTB")]
        if cfg!(feature = "FRTB") { 
            bespoke_measure_col_map
            .extend(frtb_engine::MEASURE_COL_MAP
                .iter().map(|(k, (v, ..))| 
                    (k.to_string(), v.iter().map(|h| h.to_string()).collect()) ));
            bespoke_measure_fn_map
            .extend(frtb_engine::MEASURE_COL_MAP
                .iter().map(|(k, (.., f))| 
                    (k.to_string(), *f) ));
        };
    
    (bespoke_measure_col_map, bespoke_measure_fn_map)
}

fn base(s: &String) -> Expr{
    col(s)
}