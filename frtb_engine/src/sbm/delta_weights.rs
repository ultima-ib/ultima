use polars::prelude::*;
use std::collections::HashMap;


fn rf_rw_map (c: &str, map: HashMap<String, Expr>) -> WhenThenThen {
    // buf is a placeholder
    //The only way to get WhenThenThen to build upon
    let mut buf = when(lit::<bool>(false)).then(lit::<f64>(0.).list()).when(lit::<bool>(false)).then(lit::<f64>(0.).list());
    for (k, v) in map {
        buf = buf.when(col(c).apply( move |s|{
            Ok(s.utf8()?
            .contains(k.as_str())?
            .into_series())
         }
            , GetOutput::from_type(DataType::Boolean)
        ))
        .then(v)
    }
    buf
}

pub fn delta_weight_logic(weights: DeltaWeightsConfig) -> Expr {
    let x:Option<f64> = None;
    when(col("RiskClass").eq(lit("FX")))
    .then( 
        rf_rw_map("Bucket", weights.fx_override)
        .otherwise(weights.fx)
    )

    .when(col("RiskClass").eq(lit("GIRR"))
            .and(col("RiskFactorType").eq(lit("XCCY"))
            .or(col("RiskFactorType").eq(lit("Inflation")))))
    .then( 
        //temp shortcut, since xccy weight = infl weight
        weights.ir_xccy
    )

    .when(col("RiskClass").eq(lit("GIRR"))
            .and(col("RiskFactorType").eq(lit("Yield"))))
    .then(
        rf_rw_map("Bucket", weights.ir_override)
        .otherwise(weights.ir_yield)
    )
    
    .otherwise(Series::new("c", &[x]).lit().list())

}

/// Default RWs assignment
/// @TODO: support override within request option
pub fn delta_weights_assign() -> Expr {

    let fx_base = &[0.15];
    let fx_base_srs = Series::new("", fx_base);

    let fx_map = HashMap::from([
        ("HRKEUR|BGNEUR".to_string(), Series::new("",
        &[0.05]).lit().list() ),
        ("DKKEUR".to_string(), Series::new("",
        &[0.0225]).lit().list() ),
        ("^(USD|EUR|JPY|GBP|AUD|CAD|CHF|MXN|CNY|CNO|NZD|RUB|HKD|SGD|TRY|KRW|SEK|ZAR|INR|NOK|BRL|DKK)...$".to_string(), 
        (Series::new("", fx_base)/2.0_f64.sqrt()).lit().list() ),
    ]);

    
    let ir_base = &[0., 0.017, 0.017, 0.016, 0.013, 0.012, 0.011, 0.011, 0.011, 0.011, 0.011];
    let ir_base_srs = Series::new("", ir_base);

    let ir_map = HashMap::from([
        ("EUR|USD|GBP|AUD|JPY|SEK|CAD".to_string(), (Series::new("", ir_base)/2.0_f64.sqrt()).lit().list() ),
    ]);

    let ir_xccy = Series::new("", &[0.016]);
    let ir_infl = ir_xccy.clone();

    let dlt_weights = DeltaWeightsConfig {
        fx: fx_base_srs.lit().list(),
        fx_override: fx_map,

        ir_xccy: ir_xccy.lit().list(),
        ir_yield: ir_base_srs.lit().list(),
        ir_infl: ir_infl.lit().list(),
        ir_override: ir_map,
    };
    delta_weight_logic(dlt_weights)
}


pub struct DeltaWeightsConfig {
    fx: Expr,
    fx_override: HashMap<String, Expr>,
    ir_xccy: Expr,
    ir_yield: Expr,
    ir_infl: Expr,
    ir_override: HashMap<String, Expr>,
    //eq: Expr,
}

impl Default for DeltaWeightsConfig {
    fn default() -> Self {
        DeltaWeightsConfig {
            fx: NULL.lit(), 
            fx_override: HashMap::new(),
            ir_xccy: NULL.lit(),
            ir_yield: NULL.lit(), 
            ir_infl: NULL.lit(),
            ir_override: HashMap::new(),
        }
    }
}