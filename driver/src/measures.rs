use std::collections::HashMap;
use polars::prelude::*;
use base_engine::prelude::MCMF;
use base_engine::prelude::Measure;

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

pub(crate) fn derive_measures_vec<'a> (dataset_numer_cols: &'a Vec<String>) -> Vec<Measure<'a>> {
    dataset_numer_cols
    .iter()
    .map(|x|
        Measure {
            name: x,
            // If we want to take params:
            //calculator: Box::new(|_|col(x)),
            calculator: Box::new(||col(x)),
            req_columns: vec![x],
            aggregation: None,
        }
    )
    .collect::<Vec<Measure>>()
}

// TODO: check measure_req columns are present in numer_cols from config
// if not, skip measure
pub(crate) fn derive_measures_map<'a, 'b> (dataset_numer_cols: &'a Vec<Measure<'b>>) -> HashMap<&'a str, &'a Measure<'b>> {

    let mut measure_map: HashMap<&str, &Measure> =
        dataset_numer_cols
        .iter()
        .map(|m|
            (m.name, m)
        )
        .collect();

    #[cfg(feature = "FRTB")]
    if cfg!(feature = "FRTB") { 
        measure_map
        .extend(
            frtb_engine::MEASURE_MAP
            .iter()
            .map(|m| (m.name, m))
            .collect::<HashMap<&str, &Measure>>()
        );
    };
    
    measure_map
}