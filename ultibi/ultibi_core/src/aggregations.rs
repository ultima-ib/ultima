//! This module defines supported aggregations

use std::collections::HashMap;

use derivative::Derivative;
use once_cell::sync::Lazy;
use polars::{
    lazy::dsl::lit,
    prelude::{Expr, QuantileInterpolOptions},
};

/// To represent availiable agg types living in [BASE_CALCS]
pub type AggregationName = String;
pub type FinalColumnName = String;
pub type AggregationFunction = fn(Expr, &str) -> (Expr, String);

/// The list of supported aggregations will be changing ofter, hence keep it as HashMap
pub static BASE_CALCS: Lazy<HashMap<&'static str, Aggregation>> = Lazy::new(|| {
    HashMap::from([
        //Numeric
        (
            "sum",
            Aggregation {
                name_suffix: "sum".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.sum()),
            },
        ),
        (
            "min",
            Aggregation {
                name_suffix: "min".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.min()),
            },
        ),
        (
            "max",
            Aggregation {
                name_suffix: "max".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.max()),
            },
        ),
        (
            "mean",
            Aggregation {
                name_suffix: "mean".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.mean()),
            },
        ),
        (
            "var",
            Aggregation {
                name_suffix: "var".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.var(1)),
            },
        ),
        (
            "quantile95low",
            Aggregation {
                name_suffix: "quantile_95_lower".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| {
                    e.quantile(lit(0.95), QuantileInterpolOptions::Lower)
                }),
            },
        ),
        (
            "first",
            Aggregation {
                name_suffix: "first".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.first()),
            },
        ),
        (
            "count",
            Aggregation {
                name_suffix: "count".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.count()),
            },
        ),
        (
            "n_unique",
            Aggregation {
                name_suffix: "n_unique".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e.n_unique()),
            },
        ),
        (
            "scalar",
            Aggregation {
                name_suffix: "scalar".to_string(),
                aggregated_expr_fn: Box::new(|e: Expr| e),
            },
        ),
    ])
});

type AggregationExecutor = Box<dyn Fn(Expr) -> Expr + Send + Sync>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Aggregation {
    pub name_suffix: String,
    #[derivative(Debug = "ignore")]
    pub aggregated_expr_fn: AggregationExecutor,
}

impl Aggregation {
    pub fn new_name(&self, name_buffer: &str) -> FinalColumnName {
        // scalar is special case
        if self.name_suffix != "scalar" {
            format!("{}_{}", name_buffer, self.name_suffix)
        } else {
            name_buffer.to_owned()
        }
    }
    pub fn aggregate(&self, calc: Expr, name_buffer: &str) -> Expr {
        let alias = self.new_name(name_buffer);
        let aggregated_expr = (self.aggregated_expr_fn)(calc);
        aggregated_expr.alias(&alias)
    }
}
