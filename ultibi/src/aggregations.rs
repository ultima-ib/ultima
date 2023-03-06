//! This module defines supported aggregations

use std::collections::HashMap;

use derivative::Derivative;
use once_cell::sync::Lazy;
use polars::prelude::{Expr, QuantileInterpolOptions};

/// To represent availiable agg types living in [BASE_CALCS]
pub type AggregationName = String;
pub type FinalColumnName = String;
pub type AggregationFunction = fn(Expr, &str) -> (Expr, String);

/// The list of supported aggregations will be changing ofter, hence keep it as HashMap
pub static BASE_CALCS: Lazy<HashMap<&'static str, AggregationFunction>> = Lazy::new(|| {
    HashMap::from([
        //Numeric
        ("sum", sum as fn(Expr, &str) -> (Expr, String)),
        ("min", min),
        ("max", max),
        ("mean", mean),
        ("var", var),
        ("quantile95low", quantile_95_lower),
        ("first", first),
        ("count", count),
        ("count_unique", count_unique),
        ("scalar", scalar),
    ])
});

fn sum(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_sum");
    (c.sum().alias(alias.as_ref()), alias)
}
fn min(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_min");
    (c.min().alias(alias.as_ref()), alias)
}
fn max(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_max");
    (c.max().alias(alias.as_ref()), alias)
}
fn mean(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_mean");
    (c.mean().alias(alias.as_ref()), alias)
}
fn var(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_var");
    (c.var(1).alias(alias.as_ref()), alias)
}
fn quantile_95_lower(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_quantile95lower");
    (
        c.quantile(0.95, QuantileInterpolOptions::Lower)
            .alias(alias.as_ref()),
        alias,
    )
}

fn first(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_first");
    (c.first().alias(alias.as_ref()), alias)
}

fn count(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_count");
    (c.count().alias(alias.as_ref()), alias)
}
fn count_unique(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_count_unique");
    (c.n_unique().alias(alias.as_ref()), alias)
}
/// scalar to be used how first
/// to be used with measures which have already been aggregated
/// ie calculated via apply_multiple return_scalar=true
fn scalar(c: Expr, newname: &str) -> (Expr, String) {
    (c.alias(newname), newname.to_string())
}

pub static _BASE_CALCS: Lazy<HashMap<&'static str, Aggregation>> = Lazy::new(|| {
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
                    e.quantile(0.95, QuantileInterpolOptions::Lower)
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
