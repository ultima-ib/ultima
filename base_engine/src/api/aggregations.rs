//! This module defines supported aggregations

use std::collections::HashMap;

use once_cell::sync::Lazy;
use polars::prelude::{Expr, QuantileInterpolOptions};

/// The list of supported aggregations will be changing ofter, hence keep it as HashMap
pub static BASE_CALCS: Lazy<HashMap<&'static str, fn(Expr, &str) -> (Expr, String)>> =
    Lazy::new(|| {
        HashMap::from([
            //Numeric
            ("sum", sum as fn(Expr, &str) -> (Expr, String)),
            ("min", min),
            ("max", max),
            ("mean", mean),
            ("var", var),
            ("quantile95low", quantile_95_lower),
            //All
            ("first", first),
            ("count", count),
            ("count_unique", count_unique),
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
/// Not including "_first" alias to avoid confusion
/// First is usually used by measures such as Capital or RiskWeight
/// Which are calculated at a level of a certain column such as RiskFactor
fn first(c: Expr, newname: &str) -> (Expr, String) {
    (c.first().alias(newname), newname.to_string())
}
fn count(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    (c.count().alias(alias.as_ref()), alias)
}
fn count_unique(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    (c.n_unique().alias(alias.as_ref()), alias)
}
